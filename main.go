package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic/dynamicinformer"
	dyfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/scheduler"
	"k8s.io/kubernetes/pkg/scheduler/profile"
)

func main() {
	// Node 1 not enough memory
	node1 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
			UID:  types.UID("node1"),
		},
		Spec: v1.NodeSpec{},
		Status: v1.NodeStatus{
			Capacity: v1.ResourceList{
				v1.ResourceCPU:    *(resource.NewQuantity(16, resource.DecimalSI)),
				v1.ResourceMemory: *(resource.NewQuantity(20480, resource.DecimalSI)),
				v1.ResourcePods:   *(resource.NewQuantity(10, resource.DecimalSI)),
			},
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    *(resource.NewQuantity(16, resource.DecimalSI)),
				v1.ResourceMemory: *(resource.NewQuantity(1024, resource.DecimalSI)),
				v1.ResourcePods:   *(resource.NewQuantity(10, resource.DecimalSI)),
			},
		},
	}
	// Node 2  enough memory - taints
	node2 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node2",
			UID:  types.UID("node2"),
		},
		Spec: v1.NodeSpec{
			Taints: []v1.Taint{
				{
					Key:    "system-apps",
					Value:  "true",
					Effect: "NoSchedule",
				},
			},
		},
		Status: v1.NodeStatus{
			Capacity: v1.ResourceList{
				v1.ResourceCPU:    *(resource.NewQuantity(16, resource.DecimalSI)),
				v1.ResourceMemory: *(resource.NewQuantity(20480, resource.DecimalSI)),
				v1.ResourcePods:   *(resource.NewQuantity(10, resource.DecimalSI)),
			},
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    *(resource.NewQuantity(16, resource.DecimalSI)),
				v1.ResourceMemory: *(resource.NewQuantity(8192, resource.DecimalSI)),
				v1.ResourcePods:   *(resource.NewQuantity(10, resource.DecimalSI)),
			},
		},
	}

	// Node 3  enough memory - no taints
	node3 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node3",
			UID:  types.UID("node3"),
			Labels: map[string]string{
				"failure-domain": "1",
			},
		},
		Spec: v1.NodeSpec{},
		Status: v1.NodeStatus{
			Capacity: v1.ResourceList{
				v1.ResourceCPU:    *(resource.NewQuantity(16, resource.DecimalSI)),
				v1.ResourceMemory: *(resource.NewQuantity(20480, resource.DecimalSI)),
				v1.ResourcePods:   *(resource.NewQuantity(10, resource.DecimalSI)),
			},
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    *(resource.NewQuantity(16, resource.DecimalSI)),
				v1.ResourceMemory: *(resource.NewQuantity(8192, resource.DecimalSI)),
				v1.ResourcePods:   *(resource.NewQuantity(10, resource.DecimalSI)),
			},
		},
	}

	// Node 4  enough memory - no taints
	node4 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node4",
			UID:  types.UID("node4"),
			Labels: map[string]string{
				"failure-domain": "rack-1",
			},
		},
		Spec: v1.NodeSpec{},
		Status: v1.NodeStatus{
			Capacity: v1.ResourceList{
				v1.ResourceCPU:    *(resource.NewQuantity(16, resource.DecimalSI)),
				v1.ResourceMemory: *(resource.NewQuantity(20480, resource.DecimalSI)),
				v1.ResourcePods:   *(resource.NewQuantity(10, resource.DecimalSI)),
			},
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:    *(resource.NewQuantity(16, resource.DecimalSI)),
				v1.ResourceMemory: *(resource.NewQuantity(8192, resource.DecimalSI)),
				v1.ResourcePods:   *(resource.NewQuantity(10, resource.DecimalSI)),
			},
		},
	}

	vmPods := []*v1.Pod{
		createPod("vm1"),
		createPod("vm2"),
	}

	nodes := []runtime.Object{node1, node2, node3, node4}

	defaultResync := 5 * time.Minute
	scheme := runtime.NewScheme()
	client := clientsetfake.NewSimpleClientset(nodes...)
	dynClient := dyfake.NewSimpleDynamicClient(scheme)
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	dynamicInformerFactory := dynamicinformer.NewDynamicSharedInformerFactory(dynClient, defaultResync)
	eventBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: client.EventsV1()})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//validRegistry := map[string]frameworkruntime.PluginFactory{
	//	"micro-vm": defaultbinder.New,
	//}

	s, err := scheduler.New(ctx, client,
		informerFactory,
		dynamicInformerFactory,
		profile.NewRecorderFactory(eventBroadcaster),
	)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	numPods := len(vmPods)
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
	stopFn, err := eventBroadcaster.StartEventWatcher(func(obj runtime.Object) {
		e, ok := obj.(*eventsv1.Event)
		if !ok || e.Reason != "Scheduled" {
			return
		}
		fmt.Printf("Scheduled event\n")
		controllers[e.Regarding.Name] = e.ReportingController
		wg.Done()
	})
	if err != nil {
		panic(err)
	}
	defer stopFn()

	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())

	go s.Run(ctx)

	for _, pod := range vmPods {
		_, err = client.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})
		if err != nil {
			panic(err)
		}
	}

	wg.Wait()

	pods, err := client.CoreV1().Pods("default").List(ctx, metav1.ListOptions{})
	if err != nil {
		fmt.Errorf("error getting pods")
	} else {
		for _, pod := range pods.Items {
			fmt.Printf("%s scheduled on node %s\n", pod.Name, bindings[pod.Name])
		}
	}

	fmt.Println("Finished")
}

func createPod(name string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       types.UID(name),
		},
		Spec: v1.PodSpec{
			NodeName:      "",
			SchedulerName: "default-scheduler",
			Containers: []v1.Container{
				{
					Name: "vm",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU:    *(resource.NewQuantity(2, resource.DecimalSI)),
							v1.ResourceMemory: *(resource.NewQuantity(2048, resource.DecimalSI)),
						},
						Requests: v1.ResourceList{
							v1.ResourceCPU:    *(resource.NewQuantity(2, resource.DecimalSI)),
							v1.ResourceMemory: *(resource.NewQuantity(2048, resource.DecimalSI)),
						},
					},
				},
			},
		},
	}
}
