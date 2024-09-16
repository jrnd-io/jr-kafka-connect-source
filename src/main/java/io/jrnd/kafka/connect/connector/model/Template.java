package io.jrnd.kafka.connect.connector.model;

public class Template {

    private boolean embedded;
    private String template;


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
}
