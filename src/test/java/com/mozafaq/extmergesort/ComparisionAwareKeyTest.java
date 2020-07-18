package com.mozafaq.extmergesort;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * @author Mozaffar Afaque
 */
public class ComparisionAwareKeyTest {

    @Test
    public void testCompareToEqual() {

        ComparisionAwareKey key1 = ComparisionAwareKey.create(1, 2, "12");
        ComparisionAwareKey key2 = ComparisionAwareKey.create(1, 2, "12");
        assertEquals(key1.compareTo(key2), 0);
        assertEquals(key2.compareTo(key1), 0);

        key1 = ComparisionAwareKey.create(1);
        key2 = ComparisionAwareKey.create(1);

        assertEquals(key1.compareTo(key2), 0);
        assertEquals(key2.compareTo(key1), 0);

        key1 = ComparisionAwareKey.create();
        key2 = ComparisionAwareKey.create();

        assertEquals(key1.compareTo(key2), 0);
        assertEquals(key2.compareTo(key1), 0);
    }

    @Test
    public void testCompareToNotEqual() {

        ComparisionAwareKey key1 = ComparisionAwareKey.create(1, 2);
        ComparisionAwareKey key2 = ComparisionAwareKey.create(1, 2, "12");
        assertEquals(key1.compareTo(key2), -1);
        assertEquals(key2.compareTo(key1), 1);

        key1 = ComparisionAwareKey.create(1);
        key2 = ComparisionAwareKey.create(2);

        assertEquals(key1.compareTo(key2), -1);
        assertEquals(key2.compareTo(key1), 1);

        key1 = ComparisionAwareKey.create("1", 2, "3");
        key2 = ComparisionAwareKey.create("1", 2, "4");

        assertEquals(key1.compareTo(key2), -1);
        assertEquals(key2.compareTo(key1), 1);

    }
}