package sad.project.broker.service.file;

import lombok.SneakyThrows;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashManager {
    private static volatile HashManager instance;
    private final MessageDigest messageDigest;

    private HashManager() throws NoSuchAlgorithmException {
        messageDigest = MessageDigest.getInstance("SHA-256");
    }

    @SneakyThrows
    public static HashManager getInstance() {
        if (instance == null) {
            synchronized (HashManager.class) {
                while (instance == null) {
                    instance = new HashManager();
                }
            }
        }
        return instance;
    }

    public String getMessageHash(String message) {
        byte[] hashBytes = messageDigest.digest(message.getBytes(StandardCharsets.UTF_8));
        StringBuilder hexString = new StringBuilder();
        for (byte hashByte : hashBytes) {
            String hex = Integer.toHexString(0xff & hashByte);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
}
