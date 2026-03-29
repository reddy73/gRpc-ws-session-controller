package io.session;

import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.EndpointSubset;
import io.fabric8.kubernetes.api.model.EndpointAddress;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Watches Kubernetes Endpoints objects and triggers session rebinding
 * when pod IPs change during rollouts.
 */
public class EndpointWatcher {

    private static final Logger log = Logger.getLogger(EndpointWatcher.class.getName());

    private final SessionRegistry registry;
    private final SessionRebinder rebinder;

    // Last known address -> podName mapping per namespace/service
    private final Map<String, String> lastKnownAddresses = new HashMap<>();

    public EndpointWatcher(SessionRegistry registry, SessionRebinder rebinder) {
        this.registry = registry;
        this.rebinder = rebinder;
    }

    /** Start watching Endpoints in the given namespace */
    public void watch(KubernetesClient client, String namespace) {
        client.endpoints().inNamespace(namespace).watch(new Watcher<>() {
            @Override
            public void eventReceived(Action action, Endpoints endpoints) {
                if (action == Action.MODIFIED) {
                    reconcile(endpoints, namespace);
                }
            }

            @Override
            public void onClose(WatcherException e) {
                if (e != null) log.warning("[endpoint] watcher closed: " + e.getMessage());
            }
        });
        log.info("[endpoint] watching namespace=" + namespace);
    }

    private void reconcile(Endpoints endpoints, String namespace) {
        Map<String, String> currentAddresses = extractAddresses(endpoints);

        for (Map.Entry<String, String> old : lastKnownAddresses.entrySet()) {
            String addr = old.getKey();
            String podName = old.getValue();
            if (!currentAddresses.containsKey(addr)) {
                // Address removed — rebind sessions pinned to it
                List<Session> sessions = registry.byPod(podName, namespace);
                for (Session s : sessions) {
                    currentAddresses.entrySet().stream().findFirst().ifPresent(newEntry -> {
                        log.info(String.format("[endpoint] rebinding session=%s %s -> %s",
                                s.getId(), addr, newEntry.getKey()));
                        try {
                            rebinder.rebind(s, addr, newEntry.getKey());
                        } catch (Exception e) {
                            log.warning("[endpoint] rebind error session=" + s.getId() + " " + e.getMessage());
                        }
                    });
                }
            }
        }

        lastKnownAddresses.clear();
        lastKnownAddresses.putAll(currentAddresses);
    }

    private Map<String, String> extractAddresses(Endpoints endpoints) {
        Map<String, String> result = new HashMap<>();
        if (endpoints.getSubsets() == null) return result;
        for (EndpointSubset subset : endpoints.getSubsets()) {
            if (subset.getAddresses() == null) continue;
            for (EndpointAddress addr : subset.getAddresses()) {
                String podName = addr.getTargetRef() != null ? addr.getTargetRef().getName() : "";
                result.put(addr.getIp(), podName);
            }
        }
        return result;
    }

    /** Callback interface for rebinding a session to a new endpoint */
    public interface SessionRebinder {
        void rebind(Session session, String oldEndpoint, String newEndpoint) throws Exception;
    }
}
