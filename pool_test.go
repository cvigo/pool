package pool

import (
	"testing"
	"time"
	"sync/atomic"
	"sync"
	"errors"
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

	destroy := func(r interface{}) error {
		db := r.(*resource_symulator)
		return db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	p, err := NewPool(2, 5, create, destroy, test)
	if err != nil {
		t.Fatalf("Resource error: %s", err.Error())
	}

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
	destroy := func(r interface{}) error {
		db := r.(*resource_symulator)
		return db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	p, err := NewPool(2, 5, create, destroy, test)
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

func TestWait(t *testing.T) {

	var err error

	create := func() (interface{}, error) {
		return resourceNew()
	}

	destroy := func(r interface{}) error {
		db := r.(*resource_symulator)
		return db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	p, err := NewPool(2, 5, create, destroy, test)
	if err != nil {
		t.Fatalf("Resource error: %s", err.Error())
	}

	_, err = p.Get()
	_, err = p.Get()
	_, err = p.Get()
	_, err = p.Get()
	msg, err := p.Get()

	go func() {
		msg.Close()
	}();

	msg, err = p.Get()

}

func TestExcluse(t *testing.T) {

	create := func() (interface{}, error) {
		return resourceNew()
	}

	destroy := func(r interface{}) error {
		db := r.(*resource_symulator)
		return db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	var min, max uint32
	min = 10
	max = 50

	no = 0
	p, err := NewPool(min, max, create, destroy, test)
	if err != nil {
		t.Fatal(err)
	}

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

	create := func() (interface{}, error) {
		return resourceNew()
	}

	destroy := func(r interface{}) error {
		db := r.(*resource_symulator)
		return db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	var min, max uint32
	min = 10
	max = 50
	p, err := NewPool(min, max, create, destroy, test)

	if p.Cap() != max {
		t.Fatalf("Cap size incorrect. Should be %d but is %d", max, cap(p.resources))
	}

	msg, err := p.Get()
	if err != nil {
		t.Fatalf("get error %d", err)
	}

	if p.AvailableNow() != min - 1 {
		t.Fatalf("AvailableNow size incorrect. Should be %d but is %d", min - 1, len(p.resources))
	}

	p.Release(msg)
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
		p.Destroy(v)
	}

	if p.Cap() != max {
		t.Fatalf("Pool cap incorrect. Should be %d but is %d", max, p.Cap())
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
		p.Release(dbuse[i])
	}

	if p.InUse() != 0 {
		t.Fatalf("Pool InUse() incorrect. Should be 0 but is %d", p.InUse())
	}
	if p.Cap() != max {
		t.Fatalf("Pool Cap() incorrect. Should be %d but is %d", max, p.Cap())
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
	destroy := func(r interface{}) error {
		i++
		db := r.(*resource_symulator)
		return db.resourceDel()
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

func TestError(t *testing.T) {

	var min, max uint32
	min = 10
	max = 50
	var i int = 0

	create := func() (interface{}, error) {

		if i % 2 == 2 {
			return resourceNew()
		}

		i++
		return nil, errors.New("Some error")
	}

	destroy := func(r interface{}) error {
		db := r.(*resource_symulator)
		return db.resourceDel()
	}

	test := func(r interface{}) error {
		return nil
	}

	p, err := NewPool(min, max, create, destroy, test)
	if err != nil {
		t.Fatal(err)
	}

	_, err = p.Get()
	if err == nil {
		t.Fatal("Expected Error")
	}

	for index := 0; uint32(index) < max; index++ {
		r, err := p.Get()
		if err == nil {
			go r.Close()
		}
	}

}


func TestTest(t *testing.T) {

	var min, max uint32
	min = 0
	max = 50
	var i int = 0

	create := func() (interface{}, error) {
		i++
		return resourceNew()
	}

	destroy := func(r interface{}) error {
		db := r.(*resource_symulator)
		return db.resourceDel()
	}

	test := func(r interface{}) error {
		return errors.New("Reuse Error")
	}

	p, err := NewPool(min, max, create, destroy, test)
	if err != nil {
		t.Fatal(err)
	}

	r, err := p.Get()
	if err != nil {
		t.Fatal("Expected No Error")
	}

	r.Close()
	reuse, err := p.Get()

	casted1 := r.Resource.(*resource_symulator)
	casted2 := reuse.Resource.(*resource_symulator)

	if casted1.id == casted2.id {
		t.Fatal("Expected resources not to be reused")
	}

	if i != 2 {
		t.Fatalf("Exepected two new resources to be made got %d", i)
	}
}


