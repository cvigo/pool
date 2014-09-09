// A generic resource pool for databases etc
package pool

import (
	"errors"
	"log"
	"sync"
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
	RetryTime   time.Duration
	TimeoutTime time.Duration

	fMutex sync.RWMutex //protects number of open resources
	open   uint32

	min    uint32 // Minimum Available resources
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
	p.resOpen = o
	p.resClose = c
	p.resTest = t
	p.RetryTime = time.Microsecond
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

func (p *ResourcePool) Fill() (error, bool) {

	p.fMutex.Lock()
	defer p.fMutex.Unlock()

	if p.closed {
		return PoolClosedError, false
	}

	// make sure we are not going over limit
	//our max > total open
	if p.iShouldFill() {

		resource, err := p.resOpen()

		if err != nil {

			//if we error, by definition we should still need to fill the pool
			return err, true
		}

		wrapper := ResourceWrapper{p: p, Resource: resource}
		p.resources <- wrapper
		p.open++
	}

	return nil, p.iShouldFill()
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

	for {
		r, e := p.getAvailable()
		if e == ResourceExhaustedError || e == ResourceTestError {

			//wait timeout
			if time.Now().Sub(start) > p.TimeoutTime {
				return r, e
			}

			time.Sleep(p.RetryTime)
			p.Report()
			continue
		}

		p.Report()
		p.ReportWait(time.Now().Sub(start))
		return r, e
	}

}

// Fetch / create a new resource if available
func (p *ResourcePool) getAvailable() (ResourceWrapper, error) {

	select {
	case wrapper, ok := <-p.resources:

		//pool is closed
		if !ok {
			return wrapper, PoolClosedError
		}

		//if the resource fails the test, close it and wait to get another resource
		if p.resTest(wrapper.Resource) != nil {
			p.resClose(wrapper.Resource)
			wrapper.Close()
			return ResourceWrapper{p: p, e: ResourceTestError}, ResourceTestError
		}

		//signal the filler that we need to fill
		go p.Fill()
		return wrapper, wrapper.e
	default:
	}

	//we have nothing sitting around. So we either need to wait for a new one or bail
	return ResourceWrapper{p: p, e: ResourceExhaustedError}, ResourceExhaustedError
}

/*
 * Returns a resource back in to the Pool
 */
func (p *ResourcePool) release(wrapper *ResourceWrapper) {

	//if this pool is closed when trying to release this resource
	//just close resource
	if p.closed == true {
		p.resClose(wrapper.Resource)
		wrapper.p = nil
		return
	}

	//remove resource from pool
	if p.iAvailableNow() >= p.min {

		p.fMutex.Lock()
		defer p.fMutex.Unlock()

		p.resClose(wrapper.Resource)
		p.open--
		wrapper.p = nil

	} else {

		//put it back in the available resources queue
		p.resources <- *wrapper
	}

	go p.Fill()
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
	return uint32(len(p.resources)) //p.available
}

func (p *ResourcePool) iResourcesOpen() uint32 {
	return p.open
}

func (p *ResourcePool) iCap() uint32 {
	return uint32(cap(p.resources))
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

	out := uint32(len(p.resources))
	return out
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

	out := uint32(cap(p.resources))
	return out
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
