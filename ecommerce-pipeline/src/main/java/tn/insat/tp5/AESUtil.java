package tn.insat.tp5;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;

/**
 * Utilitaire de chiffrement/dechiffrement AES-128 (ECB/PKCS5Padding).
 *
 * Utilise pour proteger les montants sensibles avant leur stockage dans HBase.
 * En production, la cle serait stockee dans un gestionnaire de secrets (Vault, AWS KMS...).
 */
public class AESUtil {

    // Cle AES-128 : 16 octets obligatoires
    private static final byte[] SECRET_KEY = "Tp5BigDataKey123".getBytes();

    /**
     * Chiffre une valeur en clair et retourne la representation Base64.
     */
    public static String encrypt(String plainText) throws Exception {
        SecretKeySpec keySpec = new SecretKeySpec(SECRET_KEY, "AES");
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE, keySpec);
        byte[] encrypted = cipher.doFinal(plainText.getBytes("UTF-8"));
        return Base64.getEncoder().encodeToString(encrypted);
    }

    /**
     * Dechiffre une valeur Base64 et retourne le texte en clair.
     */
    public static String decrypt(String cipherText) throws Exception {
        SecretKeySpec keySpec = new SecretKeySpec(SECRET_KEY, "AES");
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        cipher.init(Cipher.DECRYPT_MODE, keySpec);
        byte[] decrypted = cipher.doFinal(Base64.getDecoder().decode(cipherText));
        return new String(decrypted, "UTF-8");
    }

    /** Demonstration rapide du chiffrement. */
    public static void main(String[] args) throws Exception {
        String amount = "2847.50";
        String encrypted = encrypt(amount);
        String decrypted = decrypt(encrypted);
        System.out.println("Original  : " + amount);
        System.out.println("Chiffre   : " + encrypted);
        System.out.println("Dechiffre : " + decrypted);
    }
}
