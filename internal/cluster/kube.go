package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	vmv1beta1 "github.com/VictoriaMetrics/operator/api/operator/v1beta1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

// Access says how the tool reaches vmselect and the pods over HTTP.
type Access string

const (
	// Proxy goes through the Kubernetes API server's service and pod proxy. It works from a workstation with any
	// kubeconfig, but the API server cuts requests off after its request timeout (1m by default).
	Proxy Access = "proxy"
	// Direct connects to Service DNS names and pod IPs. Use it when the tool runs inside the cluster.
	Direct Access = "direct"
)

// Kube opens VMClusters through the Kubernetes API.
type Kube struct {
	Client kubernetes.Interface
	Access Access
	// Tenant is the tenant ID used in vmselect paths (/select/<tenant>/prometheus).
	Tenant string
	// HTTP is used for Direct access. Default: a client with a 10 minute timeout.
	HTTP *http.Client
}

// Open reads the VMCluster namespace/name and returns a Client for it.
func (k *Kube) Open(ctx context.Context, namespace, name string) (Client, error) {
	raw, err := k.Client.CoreV1().RESTClient().Get().
		AbsPath("/apis", vmv1beta1.SchemeGroupVersion.Group, vmv1beta1.SchemeGroupVersion.Version,
			"namespaces", namespace, "vmclusters", name).
		DoRaw(ctx)
	if err != nil {
		return nil, fmt.Errorf("get VMCluster %s/%s: %w", namespace, name, err)
	}
	var cr vmv1beta1.VMCluster
	if err := json.Unmarshal(raw, &cr); err != nil {
		return nil, fmt.Errorf("decode VMCluster %s/%s: %w", namespace, name, err)
	}
	if cr.Status.ParsingSpecError != "" {
		return nil, fmt.Errorf("VMCluster %s/%s: %s", namespace, name, cr.Status.ParsingSpecError)
	}
	httpClient := k.HTTP
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Minute}
	}
	return &kubeCluster{kube: k, http: httpClient, info: FromVMCluster(&cr)}, nil
}

type kubeCluster struct {
	kube *Kube
	http *http.Client
	info Info
}

func (c *kubeCluster) Info() Info { return c.info }

func (c *kubeCluster) Pods(ctx context.Context, comp Component) ([]corev1.Pod, error) {
	list, err := c.kube.Client.CoreV1().Pods(c.info.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(c.info.Selector[comp]).String(),
	})
	if err != nil {
		return nil, fmt.Errorf("list %s pods of %s: %w", comp, c.info.Name, err)
	}
	var out []corev1.Pod
	for _, p := range list.Items {
		if p.Status.Phase == corev1.PodRunning {
			out = append(out, p)
		}
	}
	return out, nil
}

func (c *kubeCluster) PodGet(ctx context.Context, pod *corev1.Pod, comp Component, path string) ([]byte, error) {
	scheme, _, port, err := c.endpoint(comp)
	if err != nil {
		return nil, err
	}
	if c.kube.Access == Direct {
		return c.get(ctx, scheme+"://"+net.JoinHostPort(pod.Status.PodIP, port)+path)
	}
	return c.kube.Client.CoreV1().Pods(pod.Namespace).ProxyGet(scheme, pod.Name, port, path, nil).DoRaw(ctx)
}

func (c *kubeCluster) SelectGet(ctx context.Context, path string, query url.Values) ([]byte, error) {
	scheme, svc, port, err := c.endpoint(Select)
	if err != nil {
		return nil, err
	}
	full := SelectPrefix(c.kube.Tenant) + "/" + strings.TrimPrefix(path, "/")
	if c.kube.Access == Direct {
		return c.get(ctx, c.info.URL[Select]+full+"?"+query.Encode())
	}
	params := map[string]string{}
	for k, v := range query {
		if len(v) > 0 {
			params[k] = v[0]
		}
	}
	return c.kube.Client.CoreV1().Services(c.info.Namespace).ProxyGet(scheme, svc, port, full, params).DoRaw(ctx)
}

func (c *kubeCluster) PDBs(ctx context.Context) ([]policyv1.PodDisruptionBudget, error) {
	list, err := c.kube.Client.PolicyV1().PodDisruptionBudgets(c.info.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

// endpoint splits a component's Service URL into scheme, Service name and port.
func (c *kubeCluster) endpoint(comp Component) (scheme, svc, port string, err error) {
	raw := c.info.URL[comp]
	if raw == "" {
		return "", "", "", fmt.Errorf("VMCluster %s has no %s", c.info.Name, comp)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", "", err
	}
	return u.Scheme, strings.SplitN(u.Hostname(), ".", 2)[0], u.Port(), nil
}

func (c *kubeCluster) get(ctx context.Context, u string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, &HTTPError{URL: u, StatusCode: resp.StatusCode, Status: resp.Status,
			Body: strings.TrimSpace(string(body[:min(len(body), 512)]))}
	}
	return body, nil
}

// HTTPError is a response other than 200 OK from a VictoriaMetrics component, reached directly.
type HTTPError struct {
	URL        string
	StatusCode int
	Status     string
	Body       string // the start of the response body
}

func (e *HTTPError) Error() string { return fmt.Sprintf("GET %s: %s: %s", e.URL, e.Status, e.Body) }

// Refused reports whether the component refused the request (401 or 403), which retrying won't fix.
func (e *HTTPError) Refused() bool {
	return e.StatusCode == http.StatusUnauthorized || e.StatusCode == http.StatusForbidden
}
