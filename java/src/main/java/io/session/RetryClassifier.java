package io.session;

import java.util.ArrayList;
import java.util.List;

/**
 * Classifies gRPC streaming RPCs and WebSocket routes by retry safety.
 * Falls back to UNSAFE if no rule matches — conservative default for streams.
 */
public class RetryClassifier {

    public record Rule(String methodPattern, Session.RetryClass retryClass) {}

    private final List<Rule> rules;

    public RetryClassifier(List<Rule> rules) {
        this.rules = rules;
    }

    public Session.RetryClass classify(String method) {
        for (Rule rule : rules) {
            if (matches(rule.methodPattern(), method)) {
                return rule.retryClass();
            }
        }
        return Session.RetryClass.UNSAFE;
    }

    /** Default rules covering common gRPC method naming conventions */
    public static List<Rule> defaultRules() {
        return List.of(
            new Rule("*/Watch",       Session.RetryClass.SAFE),
            new Rule("*/List",        Session.RetryClass.SAFE),
            new Rule("*/Get",         Session.RetryClass.SAFE),
            new Rule("*/Create",      Session.RetryClass.CONDITIONAL),
            new Rule("*/Update",      Session.RetryClass.CONDITIONAL),
            new Rule("*/Stream*",     Session.RetryClass.UNSAFE)
        );
    }

    private boolean matches(String pattern, String method) {
        if (pattern.startsWith("*/")) {
            String suffix = pattern.substring(2);
            // Support trailing wildcard e.g. "Stream*"
            if (suffix.endsWith("*")) {
                String prefix = suffix.substring(0, suffix.length() - 1);
                int slash = method.lastIndexOf('/');
                String methodName = slash >= 0 ? method.substring(slash + 1) : method;
                return methodName.startsWith(prefix);
            }
            return method.endsWith("/" + suffix);
        }
        return pattern.equals(method);
    }
}
