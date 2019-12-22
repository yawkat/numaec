# numa-eclipse-collections

This project contains implementations of some of the primitive collections from [Eclipse Collections](https://github.com/eclipse/eclipse-collections/) that are built with non-uniform memory access in mind. Data in these collections is stored in a pluggable storage backend with a similar API to ByteBuffer.

This project is experimental. It is not optimized for 
high performance yet.

numa-ec was developed for indexing functionality in [java-browser](https://java-browser.yawk.at/), where it is used to hold very large finite state automatons on disk as a more storage-efficient alternative to a database.

numa-ec is available on central:

```xml
<dependency>
    <groupId>at.yawk.numaec</groupId>
    <artifactId>numa-eclipse-collections</artifactId>
    <version>0.1</version>
</dependency>
```

## Memory Backend

All collections in this project store their data in `LargeByteBuffer`s. These have a stripped-down API similar to `ByteBuffer`, except with long-based indexing. Buffers are fixed-size and are created using `LargeByteBufferAllocator`s that are passed into the collection factories.

`BumpPointerFileAllocator` is a built-in implementation of `LargeByteBufferAllocator` that allocates chunks of memory consecutively in a temporary file. This is the backend used by java-browser.

## Lists

At the moment there is one list implementation. `IntBufferList` (and other primitive equivalents) stores its entries consecutively in a buffer, similar to `ArrayList`. This means that random access is reasonably fast, but random insertion won't be great. An additional caveat is that resizing will reallocate the entire buffer, which will not play well with `BumpPointerFileAllocator` since the old buffer will not be reused. This makes the list implementations most useful when size is known from the start.

```java
LargeByteBufferAllocator allocator = ...;
MutableIntList list = MutableIntBufferListFactory.withAllocator(allocator).empty();
list.add(1);
...
```

## Maps

There is one map implementation, `IntIntBTreeMap`. It is based on a configurable B(+)-Tree. Keys are sorted, though sort order cannot be configured at this time.

The BTree used will reuse old pages, so this collection does not have the same resizing problems as `IntBufferList`.

```java
LargeByteBufferAllocator allocator = ...;
MutableIntIntMap map = MutableIntIntBTreeMapFactory.withAllocator(allocator).empty();
map.put(1, 2);
...
```

## General notes on collections

All collections implement `BufferBasedCollection` which offers a `close()` method. Calling this method will close all buffers currently in use by this collection, which can then be reused if the storage backend supports it.

Collections are not thread-safe when writing. Concurrent reads are allowed though.
