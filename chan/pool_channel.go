// A generic resource pool for databases etc
package pool_channel

import (
	"errors"
	"sync"
	"time"

	"github.com/bountylabs/pool"
)

type resourceOpen func() (interface{}, error)
type resourceClose func(interface{})
type resourceTest func(interface{}) error

var ResourceCreationError = errors.New("Resource Creation Failed")
var ResourceExhaustedError = errors.New("Pool Exhausted")
var ResourceTestError = errors.New("Resource Test Failed")
var Timeout = errors.New("Timeout")
var PoolClosedError = errors.New("Pool is closed")

type ResourceWrapper struct {
	Resource interface{}
	p        *ResourcePool
	ticket   *int
}

func (rw ResourceWrapper) Close() {
	rw.p.release(&rw)
}

func (rw ResourceWrapper) Destroy() {
	rw.p.destroy(&rw)
}

type ResourcePool struct {
	metrics pool.PoolMetrics //metrics interface to track how the pool performs
	timeout time.Duration    //when aquiring a resource, how long should we wait before timining out

	reserve chan *ResourceWrapper //channel of available resources
	tickets chan *int             //channel of available tickets to create a resource

	//callbacks for opening, testing and closing a resource
	resOpen  func() (interface{}, error)
	resClose func(interface{}) //we can't do anything with a close error
	resTest  func(interface{}) error

	closedLock sync.RWMutex
	closed     bool
}

// NewPool creates a new pool of Clients.
func NewPool(
	maxReserve uint32,
	maxOpen uint32,
	o resourceOpen,
	c resourceClose,
	t resourceTest,
	m pool.PoolMetrics,
) *ResourcePool {

	if maxOpen < maxReserve {
		panic("maxOpen must be > maxReserve")
	}

	//create the pool
	p := &ResourcePool{
		reserve:  make(chan *ResourceWrapper, maxReserve),
		tickets:  make(chan *int, maxOpen),
		resOpen:  o,
		resClose: c,
		resTest:  t,
		timeout:  time.Second,
		metrics:  m,
	}

	//create a ticket for each possible open resource
	for i := 0; i < int(maxOpen); i++ {
		p.tickets <- &i
	}

	return p
}

func (p *ResourcePool) Get() (resource *ResourceWrapper, err error) {
	return p.GetWithTimeout(p.timeout)
}

func (p *ResourcePool) GetWithTimeout(timeout time.Duration) (resource *ResourceWrapper, err error) {

	p.closedLock.RLock()
	defer p.closedLock.RUnlock()

	//if the pool is closed we have to bail
	if p.closed {
		return nil, PoolClosedError
	}

	start := time.Now()

	for {

		if time.Now().After(start.Add(timeout)) {
			return nil, Timeout
		}

		r, e := p.getAvailable()

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

		p.report()
		p.reportWait(time.Now().Sub(start))
		return r, e
	}

}

// Borrow a Resource from the pool, create one if we can
func (p *ResourcePool) getAvailable() (*ResourceWrapper, error) {
	select {
	case r := <-p.reserve:

		//test that the re-used resource is still good
		if err := p.resTest(r.Resource); err != nil {
			return nil, ResourceTestError
		}

		return r, nil
	default:
	}

	//nothing in reserve
	return p.openNewResource()

}

func (p *ResourcePool) openNewResource() (*ResourceWrapper, error) {

	select {

	//aquire a ticket to open a resource
	case ticket := <-p.tickets:
		obj, err := p.resOpen()

		//if the open fails, return our ticket
		if err != nil {
			p.tickets <- ticket
			return nil, ResourceCreationError
		}

		return &ResourceWrapper{p: p, ticket: ticket, Resource: obj}, nil

	//if we couldn't get a ticket we have hit our max number of resources
	default:
		return nil, ResourceExhaustedError
	}

}

// Return returns a Resource to the pool.
func (p *ResourcePool) release(r *ResourceWrapper) {

	p.closedLock.RLock()
	defer p.closedLock.RUnlock()

	//if the pool is already closed just kill the resource
	if p.closed {
		p.resClose(r.Resource)
		return
	}

	//put the resource back in the cache
	select {
	case p.reserve <- r:
	default:

		//the reserve is full, close the resource and put our ticket back
		select {
		case p.tickets <- r.ticket:
			p.resClose(r.Resource)
		default:
			panic("Over Releasing Pool Resources")
		}
	}
}

// Removes a Resource
func (p *ResourcePool) destroy(r *ResourceWrapper) {

	p.closedLock.RLock()
	defer p.closedLock.RUnlock()

	//if the pool is already closed just kill the resource
	if p.closed {
		p.resClose(r.Resource)
		return
	}

	select {
	case p.tickets <- r.ticket:
		p.resClose(r.Resource)
	default:
		panic("Over Destroying Pool Resources")
	}
}

func (p *ResourcePool) Close() {

	p.closedLock.Lock()
	defer p.closedLock.Unlock()

	p.closed = true
	p.drainReserve()
	p.drainTickets()
}

func (p *ResourcePool) drainTickets() {

	for {
		select {
		case _ = <-p.tickets:
		default:
			close(p.tickets)
			return
		}
	}
}

func (p *ResourcePool) drainReserve() {

	for {
		select {
		case resource := <-p.reserve:
			p.resClose(resource.Resource)
		default:
			close(p.reserve)
			return
		}
	}
}

/**
Metrics
**/
func (p *ResourcePool) report() {
	if p.metrics != nil {
		go p.metrics.ReportResources(p.Stats())
	}
}

func (p *ResourcePool) reportWait(d time.Duration) {
	if p.metrics != nil {
		go p.metrics.ReportWait(d)
	}
}

func (p *ResourcePool) Stats() pool.ResourcePoolStat {

	open := uint32(cap(p.tickets) - len(p.tickets))
	available := uint32(len(p.reserve))

	return pool.ResourcePoolStat{
		AvailableNow:  available,
		ResourcesOpen: open,
		Cap:           uint32(cap(p.tickets)),
		InUse:         open - available,
	}
}
