package com.visorcraft.mongreldb;

/**
 * Engine / Kit failure raised by the JNI binding.
 *
 * <p>Carries the Stage 0 structural error taxonomy (FND-007 / spec 9.7):
 * {@link #getCategory()} is the stable Display name (e.g. {@code "permission
 * denied"}) and {@link #getCategoryCode()} is the never-reused numeric code in
 * {@code 1..=20}. Programmatic handling MUST key off the category or code,
 * never the message text.
 */
public class QueryException extends RuntimeException {
    private final String category;
    private final int categoryCode;

    /**
     * Legacy constructor: message only. Category defaults to
     * {@code "replica unavailable"} (code 4) so older throw sites remain
     * constructible; prefer the three-arg form.
     */
    public QueryException(String message) {
        this(message, "replica unavailable", 4);
    }

    /**
     * @param message diagnostic text (not part of the stable contract)
     * @param category taxonomy Display name, e.g. {@code "permission denied"}
     * @param categoryCode stable taxonomy code in {@code 1..=20}
     */
    public QueryException(String message, String category, int categoryCode) {
        super(message);
        this.category = category == null ? "replica unavailable" : category;
        this.categoryCode = categoryCode;
    }

    /** Stable taxonomy Display name (spec 9.7). */
    public String getCategory() {
        return category;
    }

    /**
     * Stable taxonomy code in {@code 1..=20}. Codes are never reused even if a
     * category is retired (spec section 4.10).
     */
    public int getCategoryCode() {
        return categoryCode;
    }
}
