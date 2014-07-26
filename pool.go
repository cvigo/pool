// A generic resource pool for databases etc
package pool

import (
	"errors"
	"log"
	"sync"
	"time"
)

var ResourceExhaustedError = errors.New("No Resources Available")
var PoolClosedError = errors.New("Pool is closed")

// ResourcePool allows you to use a pool of resources.
type ResourcePool struct {
	mx     sync.RWMutex
	min    uint32 // Minimum Available resources
	inUse  uint32
	closed bool

	resources chan ResourceWrapper
	resOpen   func() (interface{}, error)
	resClose  func(interface{}) //we can't do anything with a close error
	resTest   func(interface{}) error
}

type ResourceWrapper struct {
	Resource interface{}
	p        *ResourcePool
	e        error
}

func (rw ResourceWrapper) Close() {

	if rw.e != nil {
		log.Println("Can't close a bum resource")
		return
	}

	rw.p.release(&rw)
}

func (rw ResourceWrapper) Destroy() {

	if rw.e != nil {
		log.Println("Can't destroy a bum resource")
		return
	}

	rw.p.destroy(&rw)
}

/*
 * Creates a new resource Pool
 */
func NewPool(min uint32, max uint32, o func() (interface{}, error), c func(interface{}), t func(interface{}) error) (*ResourcePool, error) {

	p := new(ResourcePool)
	p.min = min

	p.resources = make(chan ResourceWrapper, max)
	p.resOpen = o
	p.resClose = c
	p.resTest = t

	var err error

	for i := uint32(0); i < min; i++ {
		resource, err := p.resOpen()
		if err == nil {
			wrapper := ResourceWrapper{p: p, Resource: resource}
			p.resources <- wrapper
		} else {
			break
		}
	}

	return p, err
}

func (p *ResourcePool) add() (err error) {

	p.mx.Lock()
	defer p.mx.Unlock()

	// make sure we are not going over limit
	//our max > total including outstanding
	for p.iAvailableNow() < p.min && p.iCap() > p.iResourcesOpen() && !p.closed {

		resource, err := p.resOpen()
		if err == nil {
			wrapper := ResourceWrapper{p: p, Resource: resource}
			p.resources <- wrapper
		} else {
			return err
		}
	}

	return nil
}

// Get will return the next available resource. If capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will indefinitely wait untill the next resource becomes available.
func (p *ResourcePool) Get() (resource ResourceWrapper, err error) {
	return p.getWait()
}

// Fetch a new resource and wait if none are available
func (p *ResourcePool) getWait() (resource ResourceWrapper, err error) {

	for {

		r, e := p.getAvailable()

		if e == ResourceExhaustedError {
			time.Sleep(time.Microsecond)
			continue
		}

		return r, e
	}

}

// Fetch / create a new resource if available
func (p *ResourcePool) getAvailable() (resource ResourceWrapper, err error) {

	p.mx.Lock()
	defer p.mx.Unlock()

	wrapper := ResourceWrapper{p: p}

	if p.closed == true {
		return wrapper, PoolClosedError
	}

	///Lets get a resource if we have some sitting around
	if p.iAvailableNow() > 0 {

		//we know the channel is open
		wrapper, _ = <-p.resources

		//make sure the resource is still okay
		//if not get a new resource
		if p.resTest(wrapper.Resource) != nil {
			p.resClose(wrapper.Resource)
			wrapper.Resource, err = p.resOpen()
		}

		//nothing current available. Lets push a new resource onto the pool
		//if our cap > total outstanding
	} else if p.iAvailableMax() > 0 {

		wrapper.Resource, err = p.resOpen()

		//We have exhausted our resources
	} else {

		return wrapper, ResourceExhaustedError
	}

	//if we couldn't get a good resource lets return the error
	if err != nil {
		//just in case
		wrapper.Resource = nil
		wrapper.e = err
		return wrapper, err
	}

	//outstanding resources++
	p.inUse++

	//incase a resource is destroyed
	//if our current available < min && our total outstanding is not less than our cap
	if p.iAvailableNow() < p.min && p.iCap() > p.iResourcesOpen() {
		go p.add()
	}

	return wrapper, err
}

/*
 * Returns a resource back in to the Pool
 */
func (p *ResourcePool) release(wrapper *ResourceWrapper) {

	p.mx.Lock()
	defer p.mx.Unlock()

	//remove resource from pool
	if p.iAvailableNow() >= p.min {

		p.resClose(wrapper.Resource)
		wrapper.p = nil

	} else {

		//put it back in the available resources queue
		p.resources <- *wrapper
	}

	//decriment how many outstanding resources we have
	p.inUse--
}

/*
 * Remove a resource from the Pool.  This is helpful if the resource
 * has gone bad.  A new resource will be created in it's place. because of add.
 */
func (p *ResourcePool) destroy(wrapper *ResourceWrapper) {

	p.mx.Lock()
	defer p.mx.Unlock()

	p.resClose(wrapper.Resource)
	p.inUse--

	wrapper.p = nil
}

// Remove all resources from the Pool.
// Then close the pool.
func (p *ResourcePool) Close() {

	p.mx.Lock()
	defer p.mx.Unlock()
	p.closed = true

	for {
		select {
		case resource := <-p.resources:
			p.resClose(resource.Resource)

		default:
			close(p.resources)
			return
		}
	}
}

/**
Unsynced Accesss
*/
func (p *ResourcePool) iAvailableNow() uint32 {
	return uint32(len(p.resources)) //p.available
}

func (p *ResourcePool) iAvailableMax() uint32 {
	return p.iCap() - p.iInUse()
}

func (p *ResourcePool) iResourcesOpen() uint32 {
	return p.iInUse() + p.iAvailableNow()
}

func (p *ResourcePool) iInUse() uint32 {
	return p.inUse
}

func (p *ResourcePool) iCap() uint32 {
	return uint32(cap(p.resources))
}

/*
Synced Access
*/
// Resources already obtained and available for use
func (p *ResourcePool) AvailableNow() uint32 {

	p.mx.Lock()
	defer p.mx.Unlock()

	out := uint32(len(p.resources))
	return out
}

// Total # of resoureces including the ones we haven't yet created - whats in use
func (p *ResourcePool) AvailableMax() uint32 {

	p.mx.Lock()
	defer p.mx.Unlock()

	out := p.iCap() - p.iInUse()
	return out
}

// number of open resources (should be less than Cap())
func (p *ResourcePool) ResourcesOpen() uint32 {

	p.mx.Lock()
	defer p.mx.Unlock()

	out := p.iInUse() + p.iAvailableNow()
	return out
}

// Resources being used right now
func (p *ResourcePool) InUse() uint32 {

	p.mx.Lock()
	defer p.mx.Unlock()

	out := p.inUse
	return out
}

// Max resources the pool allows; all in use, obtained, and not obtained.
func (p *ResourcePool) Cap() uint32 {

	p.mx.Lock()
	defer p.mx.Unlock()

	out := uint32(cap(p.resources))
	return out
}

type ResourcePoolStat struct {
	AvailableNow  uint32
	AvailableMax  uint32
	ResourcesOpen uint32
	InUse         uint32
	Cap           uint32
}

func (p *ResourcePool) Stats() ResourcePoolStat {

	p.mx.Lock()
	defer p.mx.Unlock()

	out := ResourcePoolStat{AvailableNow: p.iAvailableNow(), AvailableMax: p.iAvailableMax(), ResourcesOpen: p.iResourcesOpen(), InUse: p.iInUse(), Cap: p.iCap()}
	return out
}
