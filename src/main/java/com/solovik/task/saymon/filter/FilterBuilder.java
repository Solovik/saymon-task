package com.solovik.task.saymon.filter;

import com.solovik.task.saymon.msg.SourceMessage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;

import java.util.List;
import java.util.function.Predicate;

/**
 * Create a filtering predicate according <i>application.conf</i>.
 * Predicate can be a chain of several predicates depending on configuration.
 * There is only an <code>AND</code> condition for now, but can be easily extended.
 */
public class FilterBuilder {
    private static final String CONDITION = "condition";
    private static final String LABEL = "label";
    private static final String VALUE = "value";

    private List<? extends ConfigObject> configObjectList;

    public FilterBuilder(List<? extends ConfigObject> configObjectList) {
        this.configObjectList = configObjectList;
    }

    public Predicate<SourceMessage> createPredicate() {
        return configObjectList.stream()
                .map(this::createPredicate)
                .reduce(Predicate::and)
                .orElse(p -> true);
    }

    public Predicate<SourceMessage> createPredicate(ConfigObject configObject) {
        Config cfg = configObject.toConfig();
        System.out.println("Condition: " + cfg.getString("condition") + ", Label: " + cfg.getString("label") + ", value: " + cfg.getString("value"));

        return switch (cfg.getString(CONDITION)) {
            case "=" -> createEqualsPredicate(cfg.getString(LABEL), cfg.getString(VALUE));
            case ">" -> createGtPredicate(cfg.getString(LABEL), cfg.getInt(VALUE));
            default -> throw new UnknownFilterException("Unknown filtering condition: " + cfg.getString(CONDITION));
        };
    }

    public Predicate<SourceMessage> createEqualsPredicate(String labelKey, String value) {
        return message -> message.labels().get(labelKey).equals(value);
    }

    public Predicate<SourceMessage> createGtPredicate(String labelKey, Integer value) {
        return message -> Integer.parseInt(message.labels().get(labelKey)) > value;
    }
}
