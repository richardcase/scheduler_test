package main

import (
	"context"
	"fmt"
	v1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	clientcache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/scheduler"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/profile"
	"sync"
	"time"
)

func main() {
	// Node 1 not enough memory
	node1 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
			UID: types.UID("node1"),
		},
		Spec:       v1.NodeSpec{},
		Status:     v1.NodeStatus{
			Capacity: v1.ResourceList{
				v1.ResourceCPU: *(resource.NewQuantity(16, resource.DecimalSI)),
				v1.ResourceMemory: *(resource.NewQuantity(20480, resource.DecimalSI)),
				v1.ResourcePods: *(resource.NewQuantity(10, resource.DecimalSI)),
			},
			Allocatable: v1.ResourceList{
				v1.ResourceCPU: *(resource.NewQuantity(16, resource.DecimalSI)),
				v1.ResourceMemory: *(resource.NewQuantity(1024, resource.DecimalSI)),
				v1.ResourcePods: *(resource.NewQuantity(10, resource.DecimalSI)),
			},
		},
	}
	// Node 1  enough memory
	node2 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node2",
			UID: types.UID("node2"),
		},
		Spec:       v1.NodeSpec{},
		Status:     v1.NodeStatus{
			Capacity: v1.ResourceList{
				v1.ResourceCPU: *(resource.NewQuantity(16, resource.DecimalSI)),
				v1.ResourceMemory: *(resource.NewQuantity(20480, resource.DecimalSI)),
				v1.ResourcePods: *(resource.NewQuantity(10, resource.DecimalSI)),
			},
			Allocatable: v1.ResourceList{
				v1.ResourceCPU: *(resource.NewQuantity(16, resource.DecimalSI)),
				v1.ResourceMemory: *(resource.NewQuantity(8192, resource.DecimalSI)),
				v1.ResourcePods: *(resource.NewQuantity(10, resource.DecimalSI)),
			},
		},
	}

	vmPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:                       "vm1",
			Namespace: "default",
			UID:                        types.UID("vm1"),
		},
		Spec:       v1.PodSpec{
			NodeName: "",
			SchedulerName: "default-scheduler",
			Containers: []v1.Container{
				{
					Name: "vm",
					Resources: v1.ResourceRequirements{
						Limits:   v1.ResourceList{
							v1.ResourceCPU: *(resource.NewQuantity(2, resource.DecimalSI)),
							v1.ResourceMemory: *(resource.NewQuantity(2048, resource.DecimalSI)),
						},
						Requests: v1.ResourceList{
							v1.ResourceCPU: *(resource.NewQuantity(2, resource.DecimalSI)),
							v1.ResourceMemory: *(resource.NewQuantity(2048, resource.DecimalSI)),
						},
					},
				},
			},
		},
	}

	queuedPodStore := clientcache.NewFIFO(clientcache.MetaNamespaceKeyFunc)
	queuedPodStore.Add(vmPod)

	objs := []runtime.Object{node1, node2, vmPod}

	client := clientsetfake.NewSimpleClientset(objs...)
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	eventBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: client.EventsV1()})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//validRegistry := map[string]frameworkruntime.PluginFactory{
	//	"Foo": defaultbinder.New,
	//}

	s, err := scheduler.New(client,
		informerFactory,
		profile.NewRecorderFactory(eventBroadcaster),
		ctx.Done(),
	)
	if err != nil {
		panic(err)
	}

	s.NextPod = func() *framework.QueuedPodInfo {
		return &framework.QueuedPodInfo{Pod: clientcache.Pop(queuedPodStore).(*v1.Pod)}
	}

	var wg sync.WaitGroup
	numPods := 1
	wg.Add(2 * numPods) // Num pods
	bindings := make(map[string]string)
	client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "binding" {
			return false, nil, nil
		}
		binding := action.(clienttesting.CreateAction).GetObject().(*v1.Binding)
		bindings[binding.Name] = binding.Target.Name
		wg.Done()
		fmt.Printf("Binding, woo hoo\n")
		return true, binding, nil
	})
	controllers := make(map[string]string)
	stopFn := eventBroadcaster.StartEventWatcher(func(obj runtime.Object) {
		e, ok := obj.(*eventsv1.Event)
		if !ok || e.Reason != "Scheduled" {
			return
		}
		fmt.Printf("Scheduled event\n")
		controllers[e.Regarding.Name] = e.ReportingController
		wg.Done()
	})
	defer stopFn()

	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())

	go func() {
		for {
			pods, err := client.CoreV1().Pods("default").List(ctx, metav1.ListOptions{})
			if err != nil {
				fmt.Errorf("error getting pods")
			} else {
				for _, pod := range pods.Items {
					fmt.Printf("Pods %s scheduled on node %s\n", pod.Name, pod.Spec.NodeName)
				}
			}
			time.Sleep(2 * time.Second)
		}
	}()


	go s.Run(ctx)

	//_, err = client.CoreV1().Pods("").Create(ctx, vmPod, metav1.CreateOptions{})
	//if err != nil {
	//	panic(err)
	//}

	wg.Wait()

	pods, err := client.CoreV1().Pods("default").List(ctx, metav1.ListOptions{})
	if err != nil {
		fmt.Errorf("error getting pods")
	} else {
		for _, pod := range pods.Items {
			fmt.Printf("Pods %s scheduled on node %s\n", pod.Name, pod.Spec.NodeName)
		}
	}

	fmt.Println("Finished")

}
