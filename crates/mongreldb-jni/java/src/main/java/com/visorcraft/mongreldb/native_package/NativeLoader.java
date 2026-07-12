package com.visorcraft.mongreldb.native_package;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Loads the native MongrelDB JNI library (libmongreldb_jni) at runtime.
 *
 * <p>The library is bundled inside the JAR under
 * {@code /native/<os>/<arch>/libmongreldb_jni.<ext>} and extracted to a
 * temporary file on first use. Alternatively, the {@code MONGRELDB_NATIVE_DIR}
 * environment variable can point at a directory containing the library.</p>
 *
 * <p>This class is used internally by {@code NativeDB}; application code
 * does not call it directly.</p>
 */
public final class NativeLoader {

    private static volatile boolean loaded = false;

    private NativeLoader() {}

    /**
     * Ensures the native library is loaded. Safe to call from multiple threads.
     *
     * @throws UnsatisfiedLinkError if the library cannot be found or loaded.
     */
    public static synchronized void ensureLoaded() {
        if (loaded) return;

        // 1. Try MONGRELDB_NATIVE_DIR env var.
        String envDir = System.getenv("MONGRELDB_NATIVE_DIR");
        if (envDir != null && !envDir.isEmpty()) {
            String path = envDir + "/" + fileName();
            try {
                System.load(path);
                loaded = true;
                return;
            } catch (UnsatisfiedLinkError ignored) {
                // Fall through to bundled extraction.
            }
        }

        // 2. Try extracting from the JAR.
        String resourcePath = "/native/" + osName() + "/" + archName() + "/" + fileName();
        try (InputStream in = NativeLoader.class.getResourceAsStream(resourcePath)) {
            if (in != null) {
                Path tmp = Files.createTempFile("libmongreldb_jni", "." + ext());
                tmp.toFile().deleteOnExit();
                Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
                System.load(tmp.toString());
                loaded = true;
                return;
            }
        } catch (IOException e) {
            throw new UnsatisfiedLinkError("Failed to extract native library: " + e.getMessage());
        }

        // 3. Try system library path (System.loadLibrary).
        try {
            System.loadLibrary("mongreldb_jni");
            loaded = true;
        } catch (UnsatisfiedLinkError e) {
            throw new UnsatisfiedLinkError(
                "Could not load libmongreldb_jni. Set MONGRELDB_NATIVE_DIR or ensure " +
                "the mongreldb-jni JAR with bundled natives is on the classpath. " +
                "Resource path tried: " + resourcePath
            );
        }
    }

    private static String osName() {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("linux")) return "linux";
        if (os.contains("mac")) return "darwin";
        if (os.contains("win")) return "windows";
        return os;
    }

    private static String archName() {
        String arch = System.getProperty("os.arch").toLowerCase();
        if (arch.equals("x86_64") || arch.equals("amd64")) return "x64";
        if (arch.equals("aarch64") || arch.equals("arm64")) return "arm64";
        return arch;
    }

    private static String fileName() {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("windows")) return "mongreldb_jni." + ext();
        return "libmongreldb_jni." + ext();
    }

    private static String ext() {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("windows")) return "dll";
        if (os.contains("mac")) return "dylib";
        return "so";
    }
}
