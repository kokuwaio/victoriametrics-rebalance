// Package cluster gives the tool what it needs from one VMCluster: facts from its spec, its pods, and HTTP access
// to vmselect and to single pods.
package cluster

import (
	"context"
	"fmt"
	"net/url"

	vmv1beta1 "github.com/VictoriaMetrics/operator/api/operator/v1beta1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
)

// Component is a VMCluster component. Its value is the container name the operator uses.
type Component string

const (
	Select  Component = "vmselect"
	Insert  Component = "vminsert"
	Storage Component = "vmstorage"
)

// Components lists all components in a fixed order.
var Components = []Component{Storage, Select, Insert}

var operatorKind = map[Component]vmv1beta1.ClusterComponent{
	Select:  vmv1beta1.ClusterComponentSelect,
	Insert:  vmv1beta1.ClusterComponentInsert,
	Storage: vmv1beta1.ClusterComponentStorage,
}

// Info is what the tool reads from a VMCluster object.
type Info struct {
	Name      string
	Namespace string
	// Status is the operator's updateStatus; Operational is true when it is "operational".
	Status      string
	Operational bool
	// Retention is spec.retentionPeriod as written (VictoriaMetrics syntax, empty means one month).
	Retention         string
	ReplicationFactor int32
	StorageReplicas   int32
	// Args holds each component's extraArgs.
	Args map[Component]map[string]string
	// Selector holds the labels that select each component's pods.
	Selector map[Component]map[string]string
	// URL holds each component's Service URL, e.g. http://vmselect-x.monitoring.svc:8481.
	URL map[Component]string
}

// FromVMCluster extracts Info from a VMCluster.
func FromVMCluster(cr *vmv1beta1.VMCluster) Info {
	info := Info{
		Name:              cr.Name,
		Namespace:         cr.Namespace,
		Status:            string(cr.Status.UpdateStatus),
		Operational:       cr.Status.UpdateStatus == vmv1beta1.UpdateStatusOperational,
		Retention:         cr.Spec.RetentionPeriod,
		ReplicationFactor: 1,
		Args:              map[Component]map[string]string{},
		Selector:          map[Component]map[string]string{},
		URL:               map[Component]string{},
	}
	if cr.Spec.ReplicationFactor != nil {
		info.ReplicationFactor = *cr.Spec.ReplicationFactor
	}
	if s := cr.Spec.VMStorage; s != nil {
		if s.ReplicaCount != nil {
			info.StorageReplicas = *s.ReplicaCount
		}
		info.Args[Storage] = s.ExtraArgs
	}
	if s := cr.Spec.VMSelect; s != nil {
		info.Args[Select] = s.ExtraArgs
	}
	if s := cr.Spec.VMInsert; s != nil {
		info.Args[Insert] = s.ExtraArgs
	}
	for _, c := range Components {
		info.Selector[c] = cr.SelectorLabels(operatorKind[c])
		// AsURL returns "" for a component the spec doesn't have.
		info.URL[c] = cr.AsURL(operatorKind[c], false)
	}
	return info
}

// SelectPrefix is the path of a tenant's Prometheus API on vmselect.
func SelectPrefix(tenant string) string { return "/select/" + tenant + "/prometheus" }

// InsertPrefix is the path of a tenant's Prometheus API on vminsert.
func InsertPrefix(tenant string) string { return "/insert/" + tenant + "/prometheus" }

// Client is everything the checks need from one cluster.
type Client interface {
	Info() Info
	// Pods lists the running pods of a component.
	Pods(ctx context.Context, c Component) ([]corev1.Pod, error)
	// PodGet sends GET path to one pod of component c, on the component's HTTP port.
	PodGet(ctx context.Context, pod *corev1.Pod, c Component, path string) ([]byte, error)
	// SelectGet sends GET to vmselect, below the tenant's Prometheus API prefix (/select/<tenant>/prometheus/).
	SelectGet(ctx context.Context, path string, query url.Values) ([]byte, error)
	// PDBs lists the PodDisruptionBudgets in the cluster's namespace.
	PDBs(ctx context.Context) ([]policyv1.PodDisruptionBudget, error)
}

// ResetRollupCache clears the rollup result cache on every vmselect pod, so no pod serves results computed before
// the backfilled data arrived.
func ResetRollupCache(ctx context.Context, c Client) error {
	pods, err := RunningPods(ctx, c, Select)
	if err != nil {
		return err
	}
	for i := range pods {
		if _, err := c.PodGet(ctx, &pods[i], Select, "/internal/resetRollupResultCache"); err != nil {
			return fmt.Errorf("reset rollup cache on %s: %w", pods[i].Name, err)
		}
	}
	return nil
}

// RunningPods returns the running pods of a component, or an error if there are none.
func RunningPods(ctx context.Context, c Client, comp Component) ([]corev1.Pod, error) {
	pods, err := c.Pods(ctx, comp)
	if err != nil {
		return nil, err
	}
	if len(pods) == 0 {
		return nil, fmt.Errorf("%s has no running %s pods", c.Info().Name, comp)
	}
	return pods, nil
}
