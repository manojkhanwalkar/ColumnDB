package rest;

import java.util.regex.Pattern;

/**
 * Validates identifiers (cluster, database, table and column names) that are used to build
 * file system paths under the configured root directory. Only letters, digits, '_' and '-'
 * are allowed, so names can never contain path separators, "..", or a file extension such as
 * the ".meta" suffix used for table metadata files.
 */
public final class NameValidator {

    private static final Pattern VALID_NAME = Pattern.compile("[A-Za-z0-9_-]{1,128}");

    private NameValidator() {
    }

    public static boolean isValid(String name) {
        return name != null && VALID_NAME.matcher(name).matches();
    }

    /**
     * @throws IllegalArgumentException if the name is not a safe identifier
     */
    public static String requireValid(String name, String kind) {
        if (!isValid(name)) {
            throw new IllegalArgumentException("Invalid " + kind + " name");
        }
        return name;
    }
}
