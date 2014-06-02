// A generic resource pool for databases etc
package pool

import (
	"fmt"
	"log"
	"sync"
	"time"
	"sync/atomic"
)

// ResourcePool allows you to use a pool of resources.
type ResourcePool struct {
	mx        sync.RWMutex
	min       uint32 // Minimum Available resources
	inUse     uint32
	resources chan resourceWrapper
	resOpen   func() (interface{}, error)
	resClose  func(interface{}) error
}

type resourceWrapper struct {
	Resource interface{}
	timeUsed time.Time
	p		 *ResourcePool
}

func (rw resourceWrapper)Close() {
	rw.p.Release(rw)
}

/*
 * Creates a new resource Pool
 */
func NewPool(min uint32, max uint32, o func() (interface{}, error), c func(interface{}) error) (*ResourcePool, error) {

	p := new(ResourcePool)
	p.min = min

	p.resources = make(chan resourceWrapper, max)
	p.resOpen = o
	p.resClose = c

	var err error

	for i := uint32(0); i < min; i++ {
		resource, err := p.resOpen()
		if err == nil {
			wrapper := resourceWrapper{p:p, Resource:resource}
			p.resources <- wrapper
		} else {
			break
		}
	}

	for i := uint32(0); i < max-min; i++ {
		p.resources <- resourceWrapper{p:p}
	}

	return p, err
}

func (p *ResourcePool) add() (err error) {
	p.mx.Lock()
	defer p.mx.Unlock()

	// make sure we are not going over limit
	if p.Cap() > p.Count() {
		resource, err := p.resOpen()
		var ok bool
		if err == nil {
			wrapper := resourceWrapper{p:p, Resource:resource}
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
func (p *ResourcePool) Get() (resource resourceWrapper, err error) {
	return p.get()
}

// Fetch a new resource
func (p *ResourcePool) get() (resource resourceWrapper, err error) {
	p.mx.Lock()
	defer p.mx.Unlock()

	wrapper := resourceWrapper{p:p}
	var ok bool
	for {

		///Lets wait on a resource
		if p.AvailableNow() != 0 {
			select {
			case wrapper, ok = <-p.resources:

			default:
				panic("resource not available")
			}

			if !ok {
				return wrapper, fmt.Errorf("ResourcePool is closed")
			}

			break

		//Lets create a resource
		} else if p.AvailableMax() != 0 {

			wrapper.Resource, err = p.resOpen()
			if err == nil {
				p.resources <- wrapper
				break
			} else {
				fmt.Println("failed to open resource", err.Error())
			}
		}
	}

	//if current available < MIN &&
	if p.AvailableNow() < p.min && p.Cap() > p.Count() {
		go p.add()
	}

	atomic.AddUint32(&p.inUse, 1)
	return wrapper, err
}

/*
 * Returns a resource back in to the Pool
 */
func (p *ResourcePool) Release(wrapper resourceWrapper) {
	p.mx.Lock()
	defer p.mx.Unlock()

	//remove resource from pool
	if p.AvailableNow() > p.min {
		if err := p.resClose(wrapper.Resource); err != nil {
			log.Println("Resource close error: ", err)
		} else {
			//AddUint32(&x, ^uint32(0))
			atomic.AddUint32(&p.inUse, ^uint32(0))
		}

	//put resource back into pool
	} else {
		p.resources <- wrapper
		atomic.AddUint32(&p.inUse, ^uint32(0))
	}
}

/*
 * Remove a resource from the Pool.  This is helpful if the resource
 * has gone bad.  A new resource will be created in it's place.
 */
func (p *ResourcePool) Destroy(wrapper resourceWrapper) {
	p.mx.Lock()
	defer p.mx.Unlock()
	if err := p.resClose(wrapper.Resource); err != nil {
		log.Println("Resource close error: ", err)
	} else {
		atomic.AddUint32(&p.inUse, ^uint32(0))
	}
}

// Remove all resources from the Pool.
// Then close the pool.
func (p *ResourcePool) Close() {
	for {
		select {
		case resource := <-p.resources:
			p.resClose(resource)
		default:
			close(p.resources)
			return
		}
	}
}

// Resources already obtained and available for use
func (p *ResourcePool) AvailableNow() uint32 {
	return uint32(len(p.resources))
}

// Total # of resoureces including the once we haven't yet created - whats in use
func (p *ResourcePool) AvailableMax() uint32 {
	return p.Cap() - p.InUse()
}

// Count of resources open (should be less than Cap())
func (p *ResourcePool) Count() uint32 {
	return p.InUse() + p.AvailableNow()
}

// Resources being used right now
func (p *ResourcePool) InUse() uint32 {
	return atomic.LoadUint32(&p.inUse)
}

// Max resources the pool allows; all in use, obtained, and not obtained.
func (p *ResourcePool) Cap() uint32 {
	return uint32(cap(p.resources))
}

// Reterns how many resources we need to add to the pool, to get the reserve to reach min
func (p *ResourcePool) Short() (need uint32) {
	an := p.AvailableNow()
	if an < p.min {
		need = p.min - an
	}
	return
}
