package cluster_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/cluster"
)

const blueVMCluster = `{
  "apiVersion": "operator.victoriametrics.com/v1beta1",
  "kind": "VMCluster",
  "metadata": {"name": "blue", "namespace": "monitoring", "uid": "uid-blue"},
  "spec": {
    "retentionPeriod": "30d",
    "vmstorage": {"replicaCount": 2, "extraArgs": {"dedup.minScrapeInterval": "1ms"}},
    "vmselect": {"replicaCount": 1, "extraArgs": {"search.maxExportSeries": "20000000"}},
    "vminsert": {"replicaCount": 1, "extraArgs": {"relabelConfig": "/etc/relabel.yml"}}
  },
  "status": {"updateStatus": "operational"}
}`

// apiServer is a fake Kubernetes API server. It serves VMClusters, pods and PodDisruptionBudgets in the namespace
// monitoring, answers every proxy request with proxyBody, and records each request as "<path>?<query>".
type apiServer struct {
	vmclusters map[string]string
	pods       []corev1.Pod
	pdbs       []policyv1.PodDisruptionBudget
	proxyBody  string

	mu       sync.Mutex
	requests []string
}

func (a *apiServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	a.requests = append(a.requests, r.URL.Path+"?"+r.URL.RawQuery)
	a.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	const ns = "/api/v1/namespaces/monitoring/"
	switch p := r.URL.Path; {
	case strings.HasPrefix(p, "/apis/operator.victoriametrics.com/v1beta1/namespaces/monitoring/vmclusters/"):
		body, ok := a.vmclusters[p[strings.LastIndex(p, "/")+1:]]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(metav1.Status{Status: metav1.StatusFailure, Reason: metav1.StatusReasonNotFound, Code: http.StatusNotFound})
			return
		}
		_, _ = w.Write([]byte(body))
	case p == ns+"pods":
		_ = json.NewEncoder(w).Encode(corev1.PodList{Items: a.pods})
	case p == "/apis/policy/v1/namespaces/monitoring/poddisruptionbudgets":
		_ = json.NewEncoder(w).Encode(policyv1.PodDisruptionBudgetList{Items: a.pdbs})
	case strings.Contains(p, "/proxy/"):
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(a.proxyBody))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (a *apiServer) recorded() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.requests)
}

func newKube(t *testing.T, api *apiServer, access cluster.Access) *cluster.Kube {
	t.Helper()
	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)
	cs, err := kubernetes.NewForConfig(&rest.Config{Host: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	return &cluster.Kube{Client: cs, Access: access, Tenant: "0"}
}

func openBlue(t *testing.T, k *cluster.Kube) cluster.Client {
	t.Helper()
	c, err := k.Open(t.Context(), "monitoring", "blue")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestOpenReadsTheVMCluster(t *testing.T) {
	k := newKube(t, &apiServer{vmclusters: map[string]string{"blue": blueVMCluster}}, cluster.Proxy)

	info := openBlue(t, k).Info()

	if info.Name != "blue" || info.Namespace != "monitoring" || info.UID != "uid-blue" {
		t.Errorf("identity = %s/%s %s", info.Namespace, info.Name, info.UID)
	}
	if !info.Operational || info.Status != "operational" || info.Retention != "30d" {
		t.Errorf("status %q operational %v retention %q", info.Status, info.Operational, info.Retention)
	}
	if info.ReplicationFactor != 1 || info.StorageReplicas != 2 {
		t.Errorf("replication factor %d (want the default 1), storage replicas %d", info.ReplicationFactor, info.StorageReplicas)
	}
	if info.Args[cluster.Storage]["dedup.minScrapeInterval"] != "1ms" || info.Args[cluster.Insert]["relabelConfig"] == "" ||
		info.Args[cluster.Select]["search.maxExportSeries"] == "" {
		t.Errorf("args = %v", info.Args)
	}
	if info.URL[cluster.Select] != "http://vmselect-blue.monitoring.svc:8481" ||
		info.URL[cluster.Insert] != "http://vminsert-blue.monitoring.svc:8480" {
		t.Errorf("URLs = %v", info.URL)
	}
	for _, c := range cluster.Components {
		if s := info.Selector[c]; s["app.kubernetes.io/instance"] != "blue" || s["app.kubernetes.io/name"] != string(c) {
			t.Errorf("%s selector = %v, want this cluster's %s pods", c, s, c)
		}
	}
}

func TestOpenFailsForAMissingVMCluster(t *testing.T) {
	k := newKube(t, &apiServer{vmclusters: map[string]string{}}, cluster.Proxy)

	_, err := k.Open(t.Context(), "monitoring", "blue")

	if err == nil || !strings.Contains(err.Error(), "monitoring/blue") {
		t.Errorf("err = %v, want one naming the VMCluster", err)
	}
}

func TestOpenFailsWhenTheOperatorCantParseTheSpec(t *testing.T) {
	broken := strings.Replace(blueVMCluster, `"replicaCount": 2`, `"replicaCount": "two"`, 1)
	k := newKube(t, &apiServer{vmclusters: map[string]string{"blue": broken}}, cluster.Proxy)

	if _, err := k.Open(t.Context(), "monitoring", "blue"); err == nil {
		t.Error("want an error for a spec the operator can't parse")
	}
}

func pod(name, component string, phase corev1.PodPhase) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "monitoring",
			Labels: map[string]string{"app.kubernetes.io/name": component, "app.kubernetes.io/instance": "blue"}},
		Status: corev1.PodStatus{Phase: phase, PodIP: "10.0.0.1"},
	}
}

func TestPodsListsOnlyRunningPodsOfTheComponent(t *testing.T) {
	terminating := pod("vmstorage-blue-2", "vmstorage", corev1.PodRunning)
	now := metav1.NewTime(time.Now())
	terminating.DeletionTimestamp = &now
	api := &apiServer{
		vmclusters: map[string]string{"blue": blueVMCluster},
		pods: []corev1.Pod{
			pod("vmstorage-blue-0", "vmstorage", corev1.PodRunning),
			pod("vmstorage-blue-1", "vmstorage", corev1.PodPending),
			terminating,
		},
	}
	c := openBlue(t, newKube(t, api, cluster.Proxy))

	pods, err := c.Pods(t.Context(), cluster.Storage)
	if err != nil {
		t.Fatal(err)
	}

	if len(pods) != 1 || pods[0].Name != "vmstorage-blue-0" {
		t.Errorf("pods = %v, want only the running one that isn't terminating", names(pods))
	}
	var selector string
	for _, r := range api.recorded() {
		if strings.HasPrefix(r, "/api/v1/namespaces/monitoring/pods?") {
			q, _ := url.ParseQuery(strings.SplitN(r, "?", 2)[1])
			selector = q.Get("labelSelector")
		}
	}
	if !strings.Contains(selector, "app.kubernetes.io/instance=blue") || !strings.Contains(selector, "app.kubernetes.io/name=vmstorage") {
		t.Errorf("label selector = %q, want blue's vmstorage pods", selector)
	}
}

func names(pods []corev1.Pod) []string {
	var out []string
	for _, p := range pods {
		out = append(out, p.Name)
	}
	return out
}

func TestRunningPodsFailsWhenNoneRun(t *testing.T) {
	api := &apiServer{vmclusters: map[string]string{"blue": blueVMCluster}}
	c := openBlue(t, newKube(t, api, cluster.Proxy))

	if _, err := cluster.RunningPods(t.Context(), c, cluster.Select); err == nil ||
		!strings.Contains(err.Error(), "no running vmselect pods") {
		t.Errorf("err = %v", err)
	}
}

func TestProxyAccessGoesThroughTheAPIServer(t *testing.T) {
	api := &apiServer{vmclusters: map[string]string{"blue": blueVMCluster}, proxyBody: "answer"}
	c := openBlue(t, newKube(t, api, cluster.Proxy))
	p := pod("vminsert-blue-0", "vminsert", corev1.PodRunning)

	sel, err := c.SelectGet(t.Context(), "api/v1/query", url.Values{"query": {"up"}, "nocache": {"1"}})
	if err != nil || string(sel) != "answer" {
		t.Fatalf("SelectGet = %q, %v", sel, err)
	}
	met, err := c.PodGet(t.Context(), &p, cluster.Insert, "/metrics")
	if err != nil || string(met) != "answer" {
		t.Fatalf("PodGet = %q, %v", met, err)
	}

	reqs := api.recorded()
	wantSelect := "/api/v1/namespaces/monitoring/services/http:vmselect-blue:8481/proxy/select/0/prometheus/api/v1/query?"
	if !slices.ContainsFunc(reqs, func(r string) bool {
		return strings.HasPrefix(r, wantSelect) && strings.Contains(r, "query=up") && strings.Contains(r, "nocache=1")
	}) {
		t.Errorf("requests %q lack the vmselect Service proxy call %s with the query", reqs, wantSelect)
	}
	if !slices.Contains(reqs, "/api/v1/namespaces/monitoring/pods/http:vminsert-blue-0:8480/proxy/metrics?") {
		t.Errorf("requests %q lack the pod proxy call", reqs)
	}
}

// redirect sends every request to target and records the URL it was meant for.
type redirect struct {
	target *url.URL
	mu     sync.Mutex
	urls   []string
}

func (r *redirect) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.urls = append(r.urls, req.URL.String())
	r.mu.Unlock()
	out := req.Clone(req.Context())
	out.URL.Scheme, out.URL.Host = r.target.Scheme, r.target.Host
	return http.DefaultTransport.RoundTrip(out)
}

func TestDirectAccessUsesServiceDNSAndPodIPs(t *testing.T) {
	vm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/broken" {
			http.Error(w, "storage node unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("answer"))
	}))
	t.Cleanup(vm.Close)
	target, _ := url.Parse(vm.URL)
	rt := &redirect{target: target}
	k := newKube(t, &apiServer{vmclusters: map[string]string{"blue": blueVMCluster}}, cluster.Direct)
	k.HTTP = &http.Client{Transport: rt}
	c := openBlue(t, k)
	p := pod("vminsert-blue-0", "vminsert", corev1.PodRunning)

	if b, err := c.SelectGet(t.Context(), "/api/v1/query", url.Values{"query": {"up"}}); err != nil || string(b) != "answer" {
		t.Fatalf("SelectGet = %q, %v", b, err)
	}
	if b, err := c.PodGet(t.Context(), &p, cluster.Insert, "/metrics"); err != nil || string(b) != "answer" {
		t.Fatalf("PodGet = %q, %v", b, err)
	}
	_, err := c.PodGet(t.Context(), &p, cluster.Insert, "/broken")

	want := []string{
		"http://vmselect-blue.monitoring.svc:8481/select/0/prometheus/api/v1/query?query=up",
		"http://10.0.0.1:8480/metrics",
		"http://10.0.0.1:8480/broken",
	}
	if !slices.Equal(rt.urls, want) {
		t.Errorf("URLs = %q, want %q", rt.urls, want)
	}
	if err == nil || !strings.Contains(err.Error(), "503") || !strings.Contains(err.Error(), "storage node unavailable") {
		t.Errorf("err = %v, want the status and the body", err)
	}
}

func TestPDBsListsTheNamespacesBudgets(t *testing.T) {
	api := &apiServer{
		vmclusters: map[string]string{"blue": blueVMCluster},
		pdbs:       []policyv1.PodDisruptionBudget{{ObjectMeta: metav1.ObjectMeta{Name: "vmstorage", Namespace: "monitoring"}}},
	}
	c := openBlue(t, newKube(t, api, cluster.Proxy))

	pdbs, err := c.PDBs(t.Context())

	if err != nil || len(pdbs) != 1 || pdbs[0].Name != "vmstorage" {
		t.Errorf("PDBs = %v, %v", pdbs, err)
	}
}

func TestComponentWithoutAServiceFails(t *testing.T) {
	noSelect := strings.Replace(blueVMCluster, `"vmselect": {"replicaCount": 1, "extraArgs": {"search.maxExportSeries": "20000000"}},`, "", 1)
	k := newKube(t, &apiServer{vmclusters: map[string]string{"blue": noSelect}}, cluster.Proxy)
	c := openBlue(t, k)

	if _, err := c.SelectGet(t.Context(), "api/v1/query", nil); err == nil || !strings.Contains(err.Error(), "has no vmselect") {
		t.Errorf("err = %v", err)
	}
}
