// A generic resource pool for databases etc
package pool

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

//callbacks
type resourceOpen func() (interface{}, error)
type resourceClose func(interface{})
type resourceTest func(interface{}) error

var ResourceExhaustedError = errors.New("No Resources Available")
var ResourceTestError = errors.New("Resource Failed Reuse Test")
var PoolClosedError = errors.New("Pool is closed")

// ResourcePool allows you to use a pool of resources.
type ResourcePool struct {
	Metrics     PoolMetrics
	TimeoutTime time.Duration //when aquiring a resource, how long should we wait before timining out

	fMutex  sync.RWMutex //protects the creation and removal of resources
	open    uint32       //number of resources that are open (including in use by the client)
	maxOpen uint32       //max number of open resources

	//number of resources theoretically on the channel, this may differ from the actual length
	//while the resources are actually being created and added
	nAvailable uint32

	min    uint32 // Minimum Available resources
	closed bool

	resources chan ResourceWrapper //channel of available resources

	//callbacks for opening, testing and closing a resource
	resOpen  func() (interface{}, error)
	resClose func(interface{}) //we can't do anything with a close error
	resTest  func(interface{}) error
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
 * Caller can decide to wait on the pool to fill
 */
func NewPool(
	min uint32,
	max uint32,
	o resourceOpen,
	c resourceClose,
	t resourceTest,
	metrics PoolMetrics,
) (*ResourcePool, chan error) {

	p := new(ResourcePool)
	p.min = min

	p.resources = make(chan ResourceWrapper, max)
	p.maxOpen = max
	p.resOpen = o
	p.resClose = c
	p.resTest = t
	p.TimeoutTime = time.Second
	p.Metrics = metrics

	//fill the pool to the min
	errChannel := make(chan error, 1)
	go func() {
		defer close(errChannel)
		errChannel <- p.FillToMin()
	}()

	return p, errChannel
}

func (p *ResourcePool) iShouldFill() bool {
	return p.iAvailableNow() < p.min && p.iCap() > p.iResourcesOpen()
}

func (p *ResourcePool) Fill() error {
	return p.FillAtomic()
}

func (p *ResourcePool) FillLock() error {

	p.fMutex.Lock()
	defer p.fMutex.Unlock()

	if p.closed {
		return PoolClosedError
	}

	// make sure we are not going over limit
	//our max > total including outstanding
	if p.iShouldFill() {
		resource, err := p.resOpen()
		if err == nil {
			p.nAvailable++
			wrapper := ResourceWrapper{p: p, Resource: resource}
			p.resources <- wrapper
			p.open++
		} else {
			return err
		}
	}

	return nil
}

func (p *ResourcePool) FillAtomic() error {

	//fills should be able to run in parallel
	p.fMutex.RLock()
	defer p.fMutex.RUnlock()

	//only things that write lock, close the pool therefor we can just read the value
	if p.closed {
		return PoolClosedError
	}

	//optimistic changes
	//We assume we need to add a resource, if in the process of aquiring the locks to do so
	//we see we don't need to add a resource, we undo our lock
	if nAvailable := atomic.AddUint32(&p.nAvailable, 1); nAvailable > p.min {
		atomic.AddUint32(&p.nAvailable, ^uint32(0))
		return nil
	}

	//make sure we arn't going over our cap
	if n_open := atomic.AddUint32(&p.open, 1); n_open > p.iCap() {
		//decriment
		atomic.AddUint32(&p.nAvailable, ^uint32(0))
		atomic.AddUint32(&p.open, ^uint32(0))
		return nil
	}

	resource, err := p.resOpen()
	if err != nil {
		//decriment
		atomic.AddUint32(&p.nAvailable, ^uint32(0))
		atomic.AddUint32(&p.open, ^uint32(0))
		return err
	}

	wrapper := ResourceWrapper{p: p, Resource: resource}
	p.resources <- wrapper
	return nil
}

//used to fill the pool till we hit our min available pool size
func (p *ResourcePool) FillToMin() (err error) {

	p.fMutex.Lock()
	defer p.fMutex.Unlock()

	if p.closed {
		return PoolClosedError
	}

	// make sure we are not going over limit
	//our max > total including outstanding
	for p.iShouldFill() {
		resource, err := p.resOpen()
		if err == nil {
			//even though we are locked, we still have to increase nAvailable atomicly
			//because our get method decriments it w/o locking
			atomic.AddUint32(&p.nAvailable, 1)
			wrapper := ResourceWrapper{p: p, Resource: resource}
			p.resources <- wrapper
			p.open++
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

	start := time.Now()
	timeout := time.After(p.TimeoutTime)

	for {
		r, e := p.getAvailable(timeout)

		//if the test failed try again
		if e == ResourceTestError {
			continue
		}

		p.Report()
		p.ReportWait(time.Now().Sub(start))
		return r, e
	}

}

// Fetch / create a new resource if available
func (p *ResourcePool) getAvailable(timeout <-chan time.Time) (ResourceWrapper, error) {

	//Wait for an object, or a timeout
	select {
	case wrapper, ok := <-p.resources:

		//pool is closed
		if !ok {
			return ResourceWrapper{p: p, e: PoolClosedError}, PoolClosedError
		}

		//decriment the number of available resources
		atomic.AddUint32(&p.nAvailable, ^uint32(0))

		//if the resource fails the test, close it and wait to get another resource
		if p.resTest(wrapper.Resource) != nil {
			p.resClose(wrapper.Resource)
			wrapper.Close()
			go p.Fill()
			return ResourceWrapper{p: p, e: ResourceTestError}, ResourceTestError
		}

		//we got a valid resource to return
		//signal the filler that we need to fill
		go p.Fill()
		return wrapper, wrapper.e

	case <-timeout:
		return ResourceWrapper{p: p, e: ResourceExhaustedError}, ResourceExhaustedError
	}
}

func (p *ResourcePool) release(wrapper *ResourceWrapper) {
	p.releaseLock(wrapper)
}

func (p *ResourcePool) releaseLock(wrapper *ResourceWrapper) {

	p.fMutex.Lock()
	defer p.fMutex.Unlock()

	//if this pool is closed when trying to release this resource
	//just close the resource
	if p.closed {
		p.resClose(wrapper.Resource)
		p.open--
		wrapper.p = nil
		return
	}

	//if we have enought available
	if p.nAvailable > p.min {
		p.open--
		p.resClose(wrapper.Resource)
		wrapper.p = nil
		return
	}

	p.nAvailable++
	p.resources <- *wrapper
}

/*
 * Returns a resource back in to the Pool
 */
func (p *ResourcePool) releaseAtomic(wrapper *ResourceWrapper) {

	p.fMutex.RLock()
	defer p.fMutex.RUnlock()

	//if this pool is closed when trying to release this resource
	//just close the resource
	if p.closed == true {
		p.resClose(wrapper.Resource)
		atomic.AddUint32(&p.open, ^uint32(0))
		wrapper.p = nil
		return
	}

	//lets assume to return this resource to the pool
	//if we end up not needing to, lets undo our lock
	//more expensive check
	if nAvailable := atomic.AddUint32(&p.nAvailable, 1); nAvailable > p.min {
		//decriment
		atomic.AddUint32(&p.nAvailable, ^uint32(0))
		p.resClose(wrapper.Resource)
		atomic.AddUint32(&p.open, ^uint32(0))
		return
	}

	p.resources <- *wrapper
}

/*
 * Remove a resource from the Pool.  This is helpful if the resource
 * has gone bad.  A new resource will be created in it's place.
 */
func (p *ResourcePool) destroy(wrapper *ResourceWrapper) {

	p.fMutex.Lock()
	defer p.fMutex.Unlock()

	p.resClose(wrapper.Resource)
	p.open--
	wrapper.p = nil

	go p.Fill()
}

// Remove all resources from the Pool.
// Then close the pool.
func (p *ResourcePool) Close() {

	p.fMutex.Lock()
	defer p.fMutex.Unlock()

	p.closed = true

	for {
		select {
		case resource := <-p.resources:
			p.resClose(resource.Resource)
			p.open--
			p.nAvailable--

		default:
			close(p.resources)
			return
		}
	}
}

/**
Metrics
**/

func (p *ResourcePool) Report() {
	if p.Metrics != nil {
		go p.Metrics.ReportResources(p.iStats())
	}
}

func (p *ResourcePool) ReportWait(d time.Duration) {
	if p.Metrics != nil {
		go p.Metrics.ReportWait(d)
	}
}

/**
Unsynced Accesss
*/
func (p *ResourcePool) iAvailableNow() uint32 {
	return atomic.LoadUint32(&p.nAvailable)
}

func (p *ResourcePool) iResourcesOpen() uint32 {
	return atomic.LoadUint32(&p.open)
}

func (p *ResourcePool) iCap() uint32 {
	return p.maxOpen
}

func (p *ResourcePool) iInUse() uint32 {
	return p.open - p.iAvailableNow()
}

func (p *ResourcePool) iStats() ResourcePoolStat {
	out := ResourcePoolStat{AvailableNow: p.iAvailableNow(), ResourcesOpen: p.iResourcesOpen(), Cap: p.iCap(), InUse: p.iInUse()}
	return out
}

/*
Synced Access
*/
// Resources already obtained and available for use
func (p *ResourcePool) AvailableNow() uint32 {

	p.fMutex.RLock()
	defer p.fMutex.RUnlock()

	return atomic.LoadUint32(&p.nAvailable)
}

// number of open resources (should be less than Cap())
func (p *ResourcePool) InUse() uint32 {

	p.fMutex.RLock()
	defer p.fMutex.RUnlock()

	out := p.iInUse()
	return out
}

// number of open resources (should be less than Cap())
func (p *ResourcePool) ResourcesOpen() uint32 {

	p.fMutex.RLock()
	defer p.fMutex.RUnlock()

	out := p.iResourcesOpen()
	return out
}

// Max resources the pool allows; all in use, obtained, and not obtained.
func (p *ResourcePool) Cap() uint32 {
	return p.maxOpen
}

type ResourcePoolStat struct {
	AvailableNow  uint32
	ResourcesOpen uint32
	Cap           uint32
	InUse         uint32
}

func (p *ResourcePool) Stats() ResourcePoolStat {

	p.fMutex.RLock()
	defer p.fMutex.RUnlock()
	return p.iStats()
}
