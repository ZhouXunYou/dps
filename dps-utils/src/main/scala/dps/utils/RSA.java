package dps.utils;

import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import javax.crypto.Cipher;

import org.apache.commons.codec.binary.Base64;

public class RSA {
	private static String src = "面向对象编程，object-oriented！@#*5"; // 需要加密的原始字符串

    public static void main(String[] args) throws Exception {
//    	for(Provider provider:Security.getProviders()) {
//    		System.out.println(provider.getInfo());
//    	}
//    	System.console().readline
//    	Signature.getInstance(algorithm)
        System.out.println("初始字符串：" + src);
//        Process process = Runtime.getRuntime().exec(new String[] {});
//        process.get
        jdkRSA();

    }
    public static class ExecuteTime{
    	private long startTime;
    	public ExecuteTime(long currentTime) {
    		this.startTime = currentTime;
    	}
    	public long getConsumeTime() {
    		return System.currentTimeMillis() - this.startTime;
    	}
    }
    public static void jdkRSA() throws Exception{
        //1.初始化密钥
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        ExecuteTime initialize = new ExecuteTime(System.currentTimeMillis());
        keyPairGenerator.initialize(512);//密钥长度为64的整数倍，最大是65536
        System.out.println("initialize耗时："+initialize.getConsumeTime());
        
        ExecuteTime generateKeyPair = new ExecuteTime(System.currentTimeMillis());
        KeyPair keyPair = keyPairGenerator.generateKeyPair();
        System.out.println("generateKeyPair耗时："+generateKeyPair.getConsumeTime());
        ExecuteTime getPublicKey = new ExecuteTime(System.currentTimeMillis());
        RSAPublicKey rsaPublicKey = (RSAPublicKey) keyPair.getPublic();
        System.out.println("getPublicKey耗时："+getPublicKey.getConsumeTime());
        ExecuteTime getPrivateKey = new ExecuteTime(System.currentTimeMillis());
        RSAPrivateKey rsaPrivateKey = (RSAPrivateKey) keyPair.getPrivate();
        System.out.println("getPrivateKey耗时："+getPrivateKey.getConsumeTime());
//        System.out.println("RSA公钥：" + Base64.encodeBase64String(rsaPublicKey.getEncoded()));
//        System.out.println("RSA私钥：" + Base64.encodeBase64String(rsaPrivateKey.getEncoded()));

        //2.1私钥加密，公钥解密【加密】
        ExecuteTime pve = new ExecuteTime(System.currentTimeMillis());
        PKCS8EncodedKeySpec pkcs8EncodedKeySpec = new PKCS8EncodedKeySpec(rsaPrivateKey.getEncoded());
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PrivateKey privateKey = keyFactory.generatePrivate(pkcs8EncodedKeySpec);
        Cipher cipher = Cipher.getInstance("RSA");
        cipher.init(Cipher.ENCRYPT_MODE, privateKey);
        byte[] result = cipher.doFinal(src.getBytes());
        System.out.println("JDK RSA私钥加密：" + Base64.encodeBase64String(result)+",【私钥加密，公钥解密】加密耗时："+pve.getConsumeTime());
        
        //2.2私钥加密，公钥解密【解密】
        ExecuteTime pud = new ExecuteTime(System.currentTimeMillis());
        X509EncodedKeySpec x509EncodedKeySpec = new X509EncodedKeySpec(rsaPublicKey.getEncoded());
        keyFactory = KeyFactory.getInstance("RSA");
        PublicKey publicKey = keyFactory.generatePublic(x509EncodedKeySpec);
        cipher.init(Cipher.DECRYPT_MODE, publicKey);
        result = cipher.doFinal(result);
        System.out.println("JDK RSA公钥解密：" + new String(result)+",【私钥加密，公钥解密】解密耗时："+pud.getConsumeTime());

        //3.1公钥加密，私钥解密【加密】
        ExecuteTime pue = new ExecuteTime(System.currentTimeMillis());
        x509EncodedKeySpec = new X509EncodedKeySpec(rsaPublicKey.getEncoded());
        keyFactory = KeyFactory.getInstance("RSA");
        publicKey = keyFactory.generatePublic(x509EncodedKeySpec);
        cipher.init(Cipher.ENCRYPT_MODE, publicKey);
        result = cipher.doFinal(src.getBytes());
        System.out.println("JDK RSA公钥加密：" + Base64.encodeBase64String(result)+",【公钥加密，私钥解密】加密耗时："+pue.getConsumeTime());

        //3.2公约加密，私钥解密【解密】
        ExecuteTime pvd = new ExecuteTime(System.currentTimeMillis());
        pkcs8EncodedKeySpec =  new PKCS8EncodedKeySpec(rsaPrivateKey.getEncoded());
        keyFactory = KeyFactory.getInstance("RSA");
        privateKey = keyFactory.generatePrivate(pkcs8EncodedKeySpec);
        cipher.init(Cipher.DECRYPT_MODE, privateKey);
        result = cipher.doFinal(result);
        System.out.println("JDK RSA私钥解密：" + new String(result)+",【公钥加密，私钥解密】解密耗时："+pvd.getConsumeTime());
    }
}
