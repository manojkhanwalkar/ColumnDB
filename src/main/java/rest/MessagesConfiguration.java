package rest;

import javax.validation.constraints.NotNull;

public class MessagesConfiguration {

    @NotNull
    private String hello;

    public String getHello() {
        return hello;
    }

    public void setHello(String hello) {
        this.hello = hello;
    }
}