package com.gokgor.logworm.shell;

/** What the console shows for the selected topic. */
public enum ViewMode {
    /** Existing messages: the newest N, oldest first. */
    ALL,
    /** Only messages produced after the view starts (live tail). */
    NEW
}
