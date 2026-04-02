package io.session.endpoint;

import io.fabric8.kubernetes.api.model.EndpointAddress;
import io.fabric8.kubernetes.api.model.EndpointSubset;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.session.model.Session;
import io.session.model.SessionRegistry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Watches Kubernetes Endpoints and triggers session rebinding
 * when pod IPs change during rollouts.
 */
public class EndpointWatcher {

    private static final Logger log = Logger.getLogger(EndpointWatcher.class.getName());

    private final SessionRegistry registry;
    private final SessionRebinder rebinder;
    private final Map<String, String> lastKnownAddresses = new HashMap<>();

    public EndpointWatcher(SessionRegistry registry, SessionRebinder rebinder) {
        this.registry = registry;
        this.rebinder = rebinder;
    }

    public void watch(KubernetesClient client, String namespace) {
        client.endpoints().inNamespace(namespace).watch(new Watcher<>() {
            @Override
            public void eventReceived(Action action, Endpoints endpoints) {
                if (action == Action.MODIFIED) reconcile(endpoints, namespace);
            }

            @Override
            public void onClose(WatcherException e) {
                if (e != null) log.warning("[endpoint] watcher closed: " + e.getMessage());
            }
        });
        log.info("[endpoint] watching namespace=" + namespace);
    }

    private void reconcile(Endpoints endpoints, String namespace) {
        Map<String, String> current = extractAddresses(endpoints);

        for (Map.Entry<String, String> old : lastKnownAddresses.entrySet()) {
            if (!current.containsKey(old.getKey())) {
                List<Session> sessions = registry.byPod(old.getValue(), namespace);
                for (Session s : sessions) {
                    current.entrySet().stream().findFirst().ifPresent(newEntry -> {
                        log.info("[endpoint] rebinding session=" + s.getId()
                                + " " + old.getKey() + " -> " + newEntry.getKey());
                        try {
                            rebinder.rebind(s, old.getKey(), newEntry.getKey());
                        } catch (Exception e) {
                            log.warning("[endpoint] rebind error: " + e.getMessage());
                        }
                    });
                }
            }
        }

        lastKnownAddresses.clear();
        lastKnownAddresses.putAll(current);
    }

    private Map<String, String> extractAddresses(Endpoints endpoints) {
        Map<String, String> result = new HashMap<>();
        if (endpoints.getSubsets() == null) return result;
        for (EndpointSubset subset : endpoints.getSubsets()) {
            if (subset.getAddresses() == null) continue;
            for (EndpointAddress addr : subset.getAddresses()) {
                String pod = addr.getTargetRef() != null ? addr.getTargetRef().getName() : "";
                result.put(addr.getIp(), pod);
            }
        }
        return result;
    }

    public interface SessionRebinder {
        void rebind(Session session, String oldEndpoint, String newEndpoint) throws Exception;
    }
}
