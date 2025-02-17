package sunrise.demo.util.pic;

import javax.swing.JFrame;
import javax.swing.JPanel;

public class JumpingAppleGame extends JFrame {
    private JPanel gamePanel;

    public JumpingAppleGame() {
        super("Jumping Apple Game");
        setSize(800, 600);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        gamePanel = new JPanel();
        add(gamePanel);

        setVisible(true);
    }

    public static void main(String[] args) {
        JumpingAppleGame game = new JumpingAppleGame();

    }
}
