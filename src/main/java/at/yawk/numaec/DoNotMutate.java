package at.yawk.numaec;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Pitest marker annotation
 */
@Retention(RetentionPolicy.CLASS)
@interface DoNotMutate {
}
