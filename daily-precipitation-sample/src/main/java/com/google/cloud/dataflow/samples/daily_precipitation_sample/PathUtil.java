package com.google.cloud.dataflow.samples.daily_precipitation_sample;

import com.google.common.base.CharMatcher;

/**
 * Utility methods for manipulation of UNIX-like paths.
 */
public final class PathUtil {

  private static final CharMatcher SLASH_MATCHER = CharMatcher.is('/');
  private static final CharMatcher NON_SLASH_MATCHER = CharMatcher.isNot('/');

  private PathUtil() {}

  /**
   * Joins a set of path components into a single path.
   * Empty path components are ignored.
   *
   * @param components the components of the path
   * @return the components joined into a string, delimited by slashes.  Runs of
   *         slashes are reduced to a single slash.  If present, leading slashes
   *         on the first non-empty component and trailing slash on the last
   *         non-empty component are preserved.
   */
  public static String join(String... components) {
    int len = components.length - 1;
    if (len == -1) {
      return "";
    }
    for (String component : components) {
      len += component.length();
    }
    char[] path = new char[len];
    int i = 0;
    for (String component : components) {
      if (!component.isEmpty()) {
        if (i > 0 && path[i - 1] != '/') {
          path[i++] = '/';
        }
        for (int j = 0, end = component.length(); j < end; j++) {
          char c = component.charAt(j);
          if (!(c == '/' && i > 0 && path[i - 1] == '/')) {
            path[i++] = c;
          }
        }
      }
    }
    return new String(path, 0, i);
  }

  /**
   * Gets the final component from a path.
   *
   * Examples:
   *
   * basename("/foo/bar") = "bar"
   *
   * basename("/foo") = "foo"
   *
   * basename("/") = "/"
   *
   * @param path The path to apply the basename operation to.
   * @return path, with any leading directory elements removed.
   */
  public static String basename(String path) {
    path = removeLeadingSlashes(removeExtraneousSlashes(path));

    if (path.length() == 0) {
      return path;
    }

    int pos = path.lastIndexOf("/");
    if (pos == -1) {
      return path;
    } else {
      return path.substring(pos + 1);
    }
  }
  
  /**
   * Removes leading slashes from a string.
   */
  public static String removeLeadingSlashes(String path) {
    return CharMatcher.is('/').trimLeadingFrom(path);
  }

  /**
   * Removes trailing slashes from a string.
   */
  public static String removeTrailingSlashes(String path) {
    return CharMatcher.is('/').trimTrailingFrom(path);
  }

  /**
   * Removes extra slashes from a path.  Leading slash is preserved, trailing
   * slash is stripped, and any runs of more than one slash in the middle is
   * replaced by a single slash.
   */
  public static String removeExtraneousSlashes(String s) {
    int lastNonSlash = NON_SLASH_MATCHER.lastIndexIn(s);
    if (lastNonSlash != -1) {
      s = s.substring(0, lastNonSlash + 1);
    }

    return SLASH_MATCHER.collapseFrom(s, '/');
  }
}