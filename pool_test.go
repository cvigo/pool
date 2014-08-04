package pool

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	no int32
)

type resource_symulator struct {
	id int32
}

func resourceNew() (r *resource_symulator, err error) {

	r = new(resource_symulator)
	r.id = atomic.AddInt32(&no, 1)
	time.Sleep(time.Microsecond * 1)
	return
}

func (r *resource_symulator) resourceDel() (err error) {
	r.id = 0
	time.Sleep(time.Microsecond * 1)
	return
}

func TestIntialize(t *testing.T) {

	var err error
	create := func() (interface{}, error) {
		return resourceNew()
	}

	destroy := func(r interface{}) {
		db := r.(*resource_symulator)
		db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	p, _ := NewPool(2, 5, create, destroy, test)

	msg, err := p.Get()
	if err != nil {
		t.Fatalf("Get Resource error: %s", err.Error())
	}

	if msg.Resource.(*resource_symulator).id != 1 {
		t.Fatalf("Resource id should be on = %d", msg)
	}
}

func TestBeyond(t *testing.T) {

	var err error
	create := func() (interface{}, error) {
		return resourceNew()
	}
	destroy := func(r interface{}) {
		db := r.(*resource_symulator)
		db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	p, _ := NewPool(2, 5, create, destroy, test)
	if err != nil {
		t.Fatalf("Resource error: %s", err.Error())
	}

	_, err = p.getAvailable()
	_, err = p.getAvailable()
	_, err = p.getAvailable()
	_, err = p.getAvailable()
	_, err = p.getAvailable()
	_, err = p.getAvailable()

	if err == nil {
		t.Fatalf("Must error on sixth get")
	}

	if err != ResourceExhaustedError {
		t.Fatalf("Error must be ResourceExhaustedError")
	}
}

//Test that we don't deadlock
func TestWait(t *testing.T) {

	var err error

	create := func() (interface{}, error) {
		return resourceNew()
	}

	destroy := func(r interface{}) {
		db := r.(*resource_symulator)
		db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	p, _ := NewPool(2, 5, create, destroy, test)
	if err != nil {
		t.Fatalf("Resource error: %s", err.Error())
	}

	_, err = p.Get()
	_, err = p.Get()
	_, err = p.Get()
	_, err = p.Get()
	msg, err := p.Get()

	called := false
	go func() {
		msg.Close()
		called = true
	}()

	//this waits till msg.Close() is called in the go thread
	msg, err = p.Get()
	if !called {
		t.Fatal("Expected close of resource to block execution")
	}

}

func TestExcluse(t *testing.T) {

	create := func() (interface{}, error) {
		return resourceNew()
	}

	destroy := func(r interface{}) {
		db := r.(*resource_symulator)
		db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	var min, max uint32
	min = 10
	max = 50

	no = 0
	p, _ := NewPool(min, max, create, destroy, test)

	var waitgroup sync.WaitGroup
	check := make(map[int32]bool)

	for i := 0; i < 40; i++ {

		waitgroup.Add(1)
		go func(index int32) {

			defer waitgroup.Done()
			obj, _ := p.Get()
			casted := obj.Resource.(*resource_symulator)
			check[casted.id] = true

		}(int32(i))
	}

	waitgroup.Wait()

	for i := 1; i <= 40; i++ {
		if check[int32(i)] == false {
			t.Fatalf("Resource %d unused", i)
		}
	}

	p.Close()
}



func TestResourceRelease(t *testing.T) {

	var err error
	var destroys uint32

	create := func() (interface{}, error) {
		return resourceNew()
	}

	destroy := func(r interface{}) {

		destroys++
		db := r.(*resource_symulator)
		db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	var min, max uint32
	min = 10
	max = 50
	p, fillChan := NewPool(min, max, create, destroy, test)

	<-fillChan //wait for the pool to fill

	msg, err := p.Get()
	if err != nil {
		t.Fatalf("get error %d", err)
	}

	if p.AvailableNow() != min-1 {
		t.Fatalf("AvailableNow size incorrect. Should be %d but is %d", min-1, len(p.resources))
	}

	msg.Close()
	if min != p.AvailableNow() {
		t.Fatalf("AvailableNow size incorrect. Should be %d but is %d", min, p.AvailableNow())
	}

	var dbuse = make(map[uint32]ResourceWrapper)
	for i := uint32(0); i < max; i++ {
		dbuse[i], err = p.Get()
		if err != nil {
			t.Fatalf("get error %d", err)
		}
	}

	for _, v := range dbuse {
		v.Destroy()
	}

	if destroys != max {
		t.Fatalf("Expected %d destroys got %d", max, destroys)
	}

	// pools test
	po := uint32(50)
	for i := uint32(0); i < po; i++ {
		dbuse[i], err = p.Get()
		if err != nil {
			t.Fatalf("get error %d", err)
		}
	}

	stat := p.Stats()

	if p.InUse() != po {
		t.Fatalf("Pool InUse() before release incorrect. Should be 0 but is %d", p.InUse())
	}

	if stat.InUse != po {
		t.Fatalf("Pool InUse() before release incorrect. Should be 0 but is %d", p.InUse())
	}

	if p.AvailableMax() != p.Cap()-po {
		t.Fatalf("Pool AvailableMax() incorrect. Should be  %d but is %d", max-po, p.AvailableMax())
	}

	if stat.AvailableMax != p.Cap()-po {
		t.Fatalf("Pool AvailableMax() incorrect. Should be  %d but is %d", max-po, p.AvailableMax())
	}

	for i := uint32(0); i < po; i++ {
		value := dbuse[i]
		value.Close()
	}

	if p.InUse() != 0 {
		t.Fatalf("Pool InUse() incorrect. Should be 0 but is %d", p.InUse())
	}

	if p.AvailableNow() < min || p.AvailableNow() > max {
		t.Fatalf("Pool AvailableNow() incorrect. Should be min %d, max %d but is %d", min, max, p.AvailableNow())
	}

	if p.AvailableMax() != p.Cap() {
		t.Fatalf("Pool AvailableMax() incorrect. Should be  %d but is %d", max, p.AvailableMax())
	}
}

func TestClose(t *testing.T) {

	var min, max uint32
	min = 10
	max = 50
	var i int

	create := func() (interface{}, error) {
		return resourceNew()
	}
	destroy := func(r interface{}) {
		i++
		db := r.(*resource_symulator)
		db.resourceDel()
	}
	test := func(r interface{}) error {
		return nil
	}

	p, _ := NewPool(min, max, create, destroy, test)
	count := int(p.ResourcesOpen())
	p.Close()
	if i != count {
		t.Errorf("Close was not called correct times. It was called %d and should have been called  %d times", i, count)
	}
}

func TestPoolClose(t *testing.T) {

	var min, max uint32
	min = 10
	max = 50

	create := func() (interface{}, error) {
		return resourceNew()
	}
	destroy := func(r interface{}) {
		db := r.(*resource_symulator)
		db.resourceDel()
	}
	test := func(r interface{}) error {
		return nil
	}

	p, _ := NewPool(min, max, create, destroy, test)
	p.Close()
	_, err := p.Get()
	if err != PoolClosedError {
		t.Fatal("Expected Pool Closed Error got", err)
	}
}

func TestAddingABumResource(t *testing.T) {

	var min, max uint32
	min = 10
	max = 50
	i := 0

	create := func() (interface{}, error) {

		i++
		if i%2 == 0 {
			return nil, errors.New("Create Error")
		}

		return resourceNew()
	}

	destroy := func(r interface{}) {
		db := r.(*resource_symulator)
		db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	p, _ := NewPool(min, max, create, destroy, test)
	for index := 0; index < 50; index++ {
		func() {
			r, err := p.Get()
			if err == nil {
				defer r.Close()
			}
		}()
	}

	if p.InUse() != 0 {
		t.Fatal("Expected 0 in use")
	}

	if p.AvailableNow() >= min {
		t.Fatal("Expected availableNow to be lower than min because of errors")
	}
}

func TestCreateError(t *testing.T) {

	var min, max uint32
	min = 10
	max = 50
	var i int = 0

	create := func() (interface{}, error) {
		i++
		return nil, errors.New("Some error")
	}

	destroy := func(r interface{}) {
		db := r.(*resource_symulator)
		db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	p, _ := NewPool(min, max, create, destroy, test)

	r, _ := p.Get()
	r, _ = p.Get()

	//shouldn't do anything
	r.Close()
	r.Destroy()

	if p.InUse() != 0 {
		t.Fail()
	}

	if p.AvailableMax() != max {
		t.Fail()
	}

	//throw away
	p.Get()
	p.Get()

	stats := p.Stats()
	if stats.InUse != 0 {
		t.Fail()
	}

	if stats.AvailableNow != 0 {
		t.Fail()
	}

	if stats.AvailableMax != max {
		t.Fail()
	}

	if stats.ResourcesOpen != 0 {
		t.Fail()
	}

}

func TestTest(t *testing.T) {

	var min, max uint32
	min = 10
	max = 50
	var i uint32 = 0
	var tested uint32 = 0

	create := func() (interface{}, error) {
		i++
		return resourceNew()
	}

	destroy := func(r interface{}) {

	}

	test := func(r interface{}) error {
		tested++
		return errors.New("Reuse Error")
	}

	p, fillChannel := NewPool(min, max, create, destroy, test)
	<-fillChannel

	r, err := p.Get()
	if err != nil {
		t.Fatal("Expected No Error")
	}

	r.Close()
	reuse, err := p.Get()

	casted1 := r.Resource.(*resource_symulator)
	casted2 := reuse.Resource.(*resource_symulator)

	if tested != 2 {
		t.Fatal("Exepected one test got ", tested)
	}

	if casted1.id == casted2.id {
		t.Fatal("Expected resources not to be reused")
	}

	//every test fails plus the 10 we initialized the pool with
	if i != min+uint32(2) {
		t.Fatalf("Exepected two new resources to be made got %d", i)
	}
}
