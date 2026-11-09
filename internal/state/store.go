package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

const dataKey = "state.json"

// Store loads and changes the state. Load returns an empty State (not an error) when nothing was saved yet.
//
// Update applies change to the latest stored state and saves it. Several commands may run at once (a sync Job and a
// verify from a workstation), so each one changes only its own fields through Update and never writes back a state
// it loaded earlier.
type Store interface {
	Load(ctx context.Context) (*State, error)
	Update(ctx context.Context, change func(*State)) (*State, error)
}

// ConfigMap stores the state in the ConfigMap vmrebalance-<from>-<to>. It survives restarts of a Job's pod.
type ConfigMap struct {
	client    kubernetes.Interface
	namespace string
	name      string
	from, to  string
}

// NewConfigMap returns a store for the migration from → to in namespace.
func NewConfigMap(client kubernetes.Interface, namespace, from, to string) *ConfigMap {
	return &ConfigMap{client: client, namespace: namespace, name: Name(from, to), from: from, to: to}
}

// Name is the name of the ConfigMap that holds the state of the migration from → to.
func Name(from, to string) string { return "vmrebalance-" + from + "-" + to }

// Load reads the state, or returns an empty one if the ConfigMap doesn't exist yet.
func (c *ConfigMap) Load(ctx context.Context) (*State, error) {
	s, _, err := c.get(ctx)
	return s, err
}

// Update applies change to the stored state and saves it. When another writer saved in between, it reads the state
// again and reapplies change.
func (c *ConfigMap) Update(ctx context.Context, change func(*State)) (*State, error) {
	var out *State
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		s, cm, err := c.get(ctx)
		if err != nil {
			return err
		}
		change(s)
		data, err := json.MarshalIndent(s, "", "  ")
		if err != nil {
			return err
		}
		cms := c.client.CoreV1().ConfigMaps(c.namespace)
		if cm == nil {
			cm = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
				Name:      c.name,
				Namespace: c.namespace,
				Labels:    map[string]string{"app.kubernetes.io/managed-by": "vmrebalance"},
			}}
			cm.Data = map[string]string{dataKey: string(data)}
			_, err = cms.Create(ctx, cm, metav1.CreateOptions{})
			if apierrors.IsAlreadyExists(err) {
				// Created by another writer since we looked: retry as an update.
				return apierrors.NewConflict(corev1.Resource("configmaps"), c.name, err)
			}
		} else {
			cm.Data = map[string]string{dataKey: string(data)}
			_, err = cms.Update(ctx, cm, metav1.UpdateOptions{})
		}
		out = s
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("save ConfigMap %s/%s: %w", c.namespace, c.name, err)
	}
	return out, nil
}

// get returns the state and the ConfigMap holding it (nil if it doesn't exist yet).
func (c *ConfigMap) get(ctx context.Context) (*State, *corev1.ConfigMap, error) {
	cm, err := c.client.CoreV1().ConfigMaps(c.namespace).Get(ctx, c.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return &State{From: c.from, To: c.to}, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("get ConfigMap %s/%s: %w", c.namespace, c.name, err)
	}
	s, err := decode([]byte(cm.Data[dataKey]), c.from, c.to)
	return s, cm, err
}

// File stores the state in a local JSON file. Use it for runs from a workstation; a Job's pod loses its local
// files when it is evicted or restarted. It doesn't guard against two processes writing at once.
type File struct {
	mu       sync.Mutex
	path     string
	from, to string
}

// NewFile returns a store backed by the file at path.
func NewFile(path, from, to string) *File { return &File{path: path, from: from, to: to} }

// Load reads the state, or returns an empty one if the file doesn't exist yet.
func (f *File) Load(context.Context) (*State, error) {
	data, err := os.ReadFile(f.path)
	if errors.Is(err, fs.ErrNotExist) {
		return &State{From: f.from, To: f.to}, nil
	}
	if err != nil {
		return nil, err
	}
	return decode(data, f.from, f.to)
}

// Update applies change to the state in the file. It writes a temporary file and renames it over the old one, so a
// crash never leaves a torn file.
func (f *File) Update(ctx context.Context, change func(*State)) (*State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, err := f.Load(ctx)
	if err != nil {
		return nil, err
	}
	change(s)
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(f.path), filepath.Base(f.path)+".*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}
	return s, os.Rename(tmp.Name(), f.path)
}

func decode(data []byte, from, to string) (*State, error) {
	s := &State{}
	if len(data) > 0 {
		if err := json.Unmarshal(data, s); err != nil {
			return nil, fmt.Errorf("decode state: %w", err)
		}
	}
	if s.From != "" && (s.From != from || s.To != to) {
		return nil, fmt.Errorf("state belongs to the migration %s → %s, not %s → %s", s.From, s.To, from, to)
	}
	s.From, s.To = from, to
	return s, nil
}
