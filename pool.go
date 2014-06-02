// A generic resource pool for databases etc
package pool

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// ResourcePool allows you to use a pool of resources.
type ResourcePool struct {
	mx        sync.RWMutex
	min       uint32 // Minimum Available resources
	inUse     uint32
	resources chan ResourceWrapper
	resOpen   func() (interface{}, error)
	resClose  func(interface{}) error
}

type ResourceWrapper struct {
	Resource interface{}
	p		 *ResourcePool
}

func (rw ResourceWrapper)Close() {
	rw.p.Release(rw)
}

/*
 * Creates a new resource Pool
 */
func NewPool(min uint32, max uint32, o func() (interface{}, error), c func(interface{}) error) (*ResourcePool, error) {

	p := new(ResourcePool)
	p.min = min

	p.resources = make(chan ResourceWrapper, max)
	p.resOpen = o
	p.resClose = c

	var err error

	for i := uint32(0); i < min; i++ {
		resource, err := p.resOpen()
		if err == nil {
			wrapper := ResourceWrapper{p:p, Resource:resource}
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
	if p.iCap() > p.iCount() {
		resource, err := p.resOpen()
		var ok bool
		if err == nil {
			wrapper := ResourceWrapper{p:p, Resource:resource}
			if ok {
				p.resources <- wrapper
			}
		}
	}
	return
}

// Get will return the next available resource. If capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will indefinitely wait untill the next resource becomes available.
func (p *ResourcePool) Get() (resource ResourceWrapper, err error) {
	return p.get()
}

// Fetch a new resource
func (p *ResourcePool) get() (resource ResourceWrapper, err error) {

	p.mx.Lock()
	defer p.mx.Unlock()

	wrapper := ResourceWrapper{p:p}
	var ok bool
	for {

		///Lets get a resource if we have some sitting around
		if p.iAvailableNow() > 0 {

			wrapper, ok = <-p.resources
			if !ok {
				return wrapper, fmt.Errorf("ResourcePool is closed")
			}
			break

		//nothing current available. Lets push a new resource onto the pool
		//if our cap > total outstanding
		} else if p.iAvailableMax() > 0 {

			wrapper.Resource, err = p.resOpen()
			if err != nil {
				//just in case
				wrapper.Resource = nil
				return wrapper, err
			}

			break
		}


		time.Sleep(time.Microsecond)
	}

	//incase a resource is destroyed
	//if our current available < min && our total outstanding is not less than our cap
	if p.iAvailableNow() < p.min && p.iCap() > p.iCount() {
		go p.add()
	}

	p.inUse++
	return wrapper, err
}

/*
 * Returns a resource back in to the Pool
 */
func (p *ResourcePool) Release(wrapper ResourceWrapper) {

	p.mx.Lock()
	defer p.mx.Unlock()

	//remove resource from pool
	if p.iAvailableNow() >= p.min {

		if err := p.resClose(wrapper.Resource); err != nil {
			log.Println("Resource close error: ", err)
		}

		p.inUse--

	} else {

		p.resources <- wrapper
		p.inUse--
	}
}

/*
 * Remove a resource from the Pool.  This is helpful if the resource
 * has gone bad.  A new resource will be created in it's place. because of add.
 */
func (p *ResourcePool) Destroy(wrapper ResourceWrapper) {

	p.mx.Lock()
	defer p.mx.Unlock()

	if err := p.resClose(wrapper.Resource); err != nil {
		log.Println("Resource close error: ", err)
	} else {
		p.inUse--
	}
}

// Remove all resources from the Pool.
// Then close the pool.
func (p *ResourcePool) Close() {

	p.mx.Lock()
	defer p.mx.Unlock()

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
	return uint32(len(p.resources))
}

func (p *ResourcePool) iAvailableMax() uint32 {
	return p.iCap() - p.iInUse()
}

func (p *ResourcePool) iCount() uint32 {
	return p.iInUse() + p.iAvailableNow()
}

func (p *ResourcePool) iInUse() uint32 {
	return p.inUse;
}

func (p *ResourcePool) iCap() uint32 {
	return uint32(cap(p.resources));
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

// Count of resources open (should be less than Cap())
func (p *ResourcePool) Count() uint32 {

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

func (p *ResourcePool) Stats() string {

	p.mx.Lock()
	defer p.mx.Unlock()

	out := fmt.Sprintf("AvailableNow:%d AvailableMax:%d Count:%d, InUse:%d, Cap:%d", p.iAvailableNow(), p.iAvailableMax(), p.iCount(), p.iInUse(), p.iCap())
	return out
}

// Reterns how many resources we need to add to the pool, to get the reserve to reach min
func (p *ResourcePool) Short() (need uint32) {

	p.mx.Lock()
	defer p.mx.Unlock()

	an := p.iAvailableNow()
	if an < p.min {
		need = p.min - an
	}
	return
}
