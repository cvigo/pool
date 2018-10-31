// A generic resource pool for databases etc
package pool

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

//callbacks
type resourceOpen func(ctx context.Context) (interface{}, error)
type resourceClose func(context.Context, interface{})
type resourceTest func(context.Context, interface{}) error

var ResourceExhaustedError = errors.New("No Resources Available")
var ResourceCreationError = errors.New("Resource creation failed")
var ResourceTimeoutError = errors.New("Resource Get Timeout")
var ResourceTestError = errors.New("Resource Failed Reuse Test")
var PoolClosedError = errors.New("Pool is closed")

// ResourcePool allows you to use a pool of resources.
type ResourcePool struct {
	Metrics     PoolMetrics
	TimeoutTime time.Duration //when aquiring a resource, how long should we wait before timining out

	fMutex  sync.RWMutex //protects the creation and removal of resources
	open    uint32       //number of resources that are open (including in use by the client)
	maxOpen uint32       //max number of open resources

	purgeFailed bool // destroy the resources which test have failed

	//number of resources theoretically on the channel, this may differ from the actual length
	//while the resources are actually being created and added
	nAvailable uint32

	min    uint32 // Minimum Available resources
	closed bool

	resources chan ResourceWrapper //channel of available resources

	//callbacks for opening, testing and closing a resource
	resOpen  func(ctx context.Context) (interface{}, error)
	resClose func(context.Context, interface{}) //we can't do anything with a close error
	resTest  func(context.Context, interface{}) error
}

type ResourceWrapper struct {
	Resource interface{}
	p        *ResourcePool
	e        error
}

func (rw ResourceWrapper) Release(ctx context.Context) {

	if rw.e != nil {
		log.Println("Can't close a bum resource")
		return
	}

	rw.p.release(ctx, &rw)
}

func (rw ResourceWrapper) Destroy(ctx context.Context) {

	if rw.e != nil {
		log.Println("Can't destroy a bum resource")
		return
	}

	rw.p.destroy(ctx, &rw)
}

type Cfg struct {
	Min         uint32
	Max         uint32
	OpenFunc    resourceOpen
	CloseFunc   resourceClose
	TestFunc    resourceTest
	Metrics     PoolMetrics
	PurgeFailed bool
}

/*
 * Creates a new resource Pool
 * Caller can decide to wait on the pool to fill
 */
func NewPool(
	ctx context.Context,
	cfg Cfg,
) (*ResourcePool, chan error) {

	p := new(ResourcePool)
	p.min = cfg.Min

	p.resources = make(chan ResourceWrapper, cfg.Max)
	p.maxOpen = cfg.Max
	p.resOpen = cfg.OpenFunc
	p.resClose = cfg.CloseFunc
	p.resTest = cfg.TestFunc
	p.TimeoutTime = time.Second
	p.Metrics = cfg.Metrics
	p.purgeFailed = cfg.PurgeFailed

	//fill the pool to the min
	errChannel := make(chan error, 1)
	go func() {
		defer close(errChannel)
		errChannel <- p.FillToMin(ctx)
	}()

	return p, errChannel
}

func (p *ResourcePool) iShouldFill() bool {
	return p.iAvailableNow() < p.min && p.Cap() > p.iResourcesOpen()
}

//used to fill the pool till we hit our min available pool size
func (p *ResourcePool) FillToMin(ctx context.Context) (err error) {

	p.fMutex.Lock()
	defer p.fMutex.Unlock()

	if p.closed {
		return PoolClosedError
	}

	for {
		//obtain the lock for increasing the number of available resources
		if nAvailable := atomic.AddUint32(&p.nAvailable, 1); nAvailable > p.min {
			//decrement
			atomic.AddUint32(&p.nAvailable, ^uint32(0))
			return
		}

		//obtain the lock for increasnig the total number of open resources
		if nOpen := atomic.AddUint32(&p.open, 1); nOpen > p.Cap() {
			//decrement
			atomic.AddUint32(&p.nAvailable, ^uint32(0))
			atomic.AddUint32(&p.open, ^uint32(0))
			return
		}

		resource, err := p.resOpen(ctx)
		if err != nil {
			atomic.AddUint32(&p.nAvailable, ^uint32(0))
			atomic.AddUint32(&p.open, ^uint32(0))
			return err
		}

		wrapper := ResourceWrapper{p: p, Resource: resource}
		p.resources <- wrapper
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// carry on
		}
	}

	return nil
}

// Get will return the next available resource. If capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will indefinitely wait until the next resource becomes available.
func (p *ResourcePool) Get(ctx context.Context) (resource ResourceWrapper, err error) {
	return p.getWait(ctx)
}

// Fetch a new resource and wait if none are available
func (p *ResourcePool) getWait(ctx context.Context) (resource ResourceWrapper, err error) {

	start := time.Now()

	for {
		r, e := p.getAvailable(ctx)

		//if the test failed try again
		if e == ResourceTestError {
			time.Sleep(time.Microsecond)
			continue
		}

		//if we are at our max open try again after a short sleep
		if e == ResourceExhaustedError {
			time.Sleep(time.Microsecond)
			continue
		}

		//if we failed to create a new resource, try agaig after a short sleep
		if e == ResourceCreationError {
			time.Sleep(time.Microsecond)
			continue
		}

		p.Report()
		p.ReportWait(time.Now().Sub(start))
		return r, e
	}

}

// Fetch / create a new resource if available
// Its technically possible to create a new resource after the pool is closed
// thats okay, its still the callers responsibility to close/destroy that resource
func (p *ResourcePool) getAvailable(ctx context.Context) (ResourceWrapper, error) {

	//Wait for an object, or a timeout
	select {
	case <-ctx.Done():
		return ResourceWrapper{p: p, e: ctx.Err()}, ctx.Err()

	case wrapper, ok := <-p.resources:

		//pool is closed
		if !ok {
			return ResourceWrapper{p: p, e: PoolClosedError}, PoolClosedError
		}

		//decrement the number of available resources
		atomic.AddUint32(&p.nAvailable, ^uint32(0))

		//if the resource fails the test, close it and wait to get another resource
		if p.resTest(ctx, wrapper.Resource) != nil {
			p.resClose(ctx, wrapper.Resource)
			if p.purgeFailed {
				wrapper.Destroy(ctx)
			} else {
				wrapper.Release(ctx)
			}
			return ResourceWrapper{p: p, e: ResourceTestError}, ResourceTestError
		}

		//we got a valid resource to return
		//signal the filler that we need to fill
		return wrapper, wrapper.e

		//we don't have a resource available
		//lets create one if we can
	default:

		//try to obtain a lock for a new resource
		if n_open := atomic.AddUint32(&p.open, 1); n_open > p.Cap() {
			//decriment
			atomic.AddUint32(&p.open, ^uint32(0))
			return ResourceWrapper{p: p, e: ResourceExhaustedError}, ResourceExhaustedError
		}

		resource, err := p.resOpen(ctx)
		if err != nil {
			//decriment
			atomic.AddUint32(&p.open, ^uint32(0))
			return ResourceWrapper{p: p, e: ResourceCreationError}, ResourceCreationError
		}

		return ResourceWrapper{p: p, Resource: resource}, nil
	}
}

func (p *ResourcePool) release(ctx context.Context, wrapper *ResourceWrapper) {
	p.releaseAtomic(ctx, wrapper)
}

/*
 * Returns a resource back in to the Pool
 */
func (p *ResourcePool) releaseAtomic(ctx context.Context, wrapper *ResourceWrapper) {

	p.fMutex.RLock()
	defer p.fMutex.RUnlock()

	//if this pool is closed when trying to release this resource
	//just close the resource
	if p.closed == true {
		p.resClose(ctx, wrapper.Resource)
		atomic.AddUint32(&p.open, ^uint32(0))
		wrapper.p = nil
		return
	}

	//obtain a lock to return the resource to the pool
	//if we end up not needing to, lets undo our lock
	//and close the resource
	if nAvailable := atomic.AddUint32(&p.nAvailable, 1); nAvailable > p.min {
		//decriment
		atomic.AddUint32(&p.nAvailable, ^uint32(0))
		p.resClose(ctx, wrapper.Resource)
		atomic.AddUint32(&p.open, ^uint32(0))
		return
	}

	p.resources <- *wrapper
}

/*
 * Remove a resource from the Pool.  This is helpful if the resource
 * has gone bad.  A new resource will be created in it's place. once its asked for
 * IE its possible to destroy resources in a pool bellow its min value
 */
func (p *ResourcePool) destroy(ctx context.Context, wrapper *ResourceWrapper) {

	//you can destroy a resource if the pool is closed, no harm no foul
	p.resClose(ctx, wrapper.Resource)
	atomic.AddUint32(&p.open, ^uint32(0))
	wrapper.p = nil
}

// Remove all resources from the Pool.
// Then close the pool.
func (p *ResourcePool) Close(ctx context.Context) {

	p.fMutex.Lock()
	defer p.fMutex.Unlock()

	p.closed = true

	for {
		select {
		case resource := <-p.resources:
			p.resClose(ctx, resource.Resource)
			atomic.AddUint32(&p.nAvailable, ^uint32(0))
			atomic.AddUint32(&p.open, ^uint32(0))
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

func (p *ResourcePool) iInUse() uint32 {
	return p.iResourcesOpen() - p.iAvailableNow()
}

func (p *ResourcePool) iStats() ResourcePoolStat {
	out := ResourcePoolStat{AvailableNow: p.iAvailableNow(), ResourcesOpen: p.iResourcesOpen(), Cap: p.Cap(), InUse: p.iInUse()}
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
