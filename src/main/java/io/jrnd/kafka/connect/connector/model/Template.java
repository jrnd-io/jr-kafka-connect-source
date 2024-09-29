package io.jrnd.kafka.connect.connector.model;

public class Template {

    private boolean embedded;
    private boolean keyEmbedded;
    private String template;
    private String keyTemplate;


    public Template() {}

    public boolean isEmbedded() {
        return embedded;
    }

    public String getTemplate() {
        return template;
    }

    public void setEmbedded(boolean embedded) {
        this.embedded = embedded;
    }

    public void setTemplate(String template) {
        this.template = template;
    }

    public boolean isKeyEmbedded() {
        return keyEmbedded;
    }

    public void setKeyEmbedded(boolean keyEmbedded) {
        this.keyEmbedded = keyEmbedded;
    }

    public String getKeyTemplate() {
        return keyTemplate;
    }

    public void setKeyTemplate(String keyTemplate) {
        this.keyTemplate = keyTemplate;
    }
}
