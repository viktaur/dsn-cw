public class Message {

    /**
     * Content of the message received
     */
    private final String content;

    /**
     * Thread from which the message was received
     */
    private final ConnectionThread sender;

    public Message(String content, ConnectionThread sender) {
        this.content = content;
        this.sender = sender;
    }

    public String getContent() {
        return content;
    }

    public ConnectionThread getSender() {
        return sender;
    }
}
