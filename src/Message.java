public class Message {

    /**
     * Content of the message received
     */
    private final String content;

    /**
     * Thread from which the message was received
     */
    private final NetworkController.ConnectionThread sender;

    public Message(String content, NetworkController.ConnectionThread sender) {
        this.content = content;
        this.sender = sender;
    }

    public String getContent() {
        return content;
    }

    public NetworkController.ConnectionThread getSender() {
        return sender;
    }
}
