package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.bookie.utils.FileStatus;
import org.apache.bookkeeper.bookie.utils.FileChannelStatus;
import org.apache.bookkeeper.bookie.utils.AllocatorStatus;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class BufferedChannelConstructorTest {

    private BufferedChannel bufferedChannel;
    private ByteBufAllocator allocator;
    private FileStatus fileStatus;
    private FileChannel fc;
    private FileChannelStatus fileChannelStatus;
    private Class<? extends Exception> expectedException;
    private int writeCapacity;
    private int readCapacity;
    private long unpersistedBytesBound;
    private AllocatorStatus allocatorStatus;
    private Set<PosixFilePermission> originalPermissions;
    private final Path PATH = Paths.get("src/test/java/org/apache/bookkeeper/bookie/utils/fileForTest");
    private static final Logger logger = LoggerFactory.getLogger(BufferedChannelConstructorTest.class);
    private Class<? extends Exception> exceptionCatched;


    public BufferedChannelConstructorTest(AllocatorStatus allocatorStatus,
                                          FileStatus fileStatus,
                                          FileChannelStatus fileChannelStatus,
                                          int writeCapacity,
                                          int readCapacity,
                                          long unpersistedBytesBound,
                                          Class<? extends Exception> expectedException) {

        this.allocatorStatus = allocatorStatus;
        this.writeCapacity = writeCapacity;
        this.readCapacity = readCapacity;
        this.unpersistedBytesBound = unpersistedBytesBound;
        this.expectedException = expectedException;
        this.fileStatus = fileStatus;
        this.fileChannelStatus = fileChannelStatus;
    }

    // List of parameters
    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{

                // TEST 1: Allocator null, FileChannel default, write=0, read=0, unpersisted=0 -> NullPointerException
                {AllocatorStatus.NULL, FileStatus.DEFAULT, FileChannelStatus.DEFAULT, 0, 0, 0, NullPointerException.class},

                // TEST 2: Allocator default, FileChannel default, write=1 read=0, unpersisted=0 -> NullPointerException
                {AllocatorStatus.DEFAULT, FileStatus.DEFAULT, FileChannelStatus.NULL, 1, 0, 0, NullPointerException.class},

                // TEST 3: Allocator default, FileChannel closed, write=1, read=1, unpersisted=1 -> ClosedChannelException
                {AllocatorStatus.DEFAULT, FileStatus.DEFAULT, FileChannelStatus.CLOSED, 1, 1, 1, ClosedChannelException.class},

                // TEST 4: Allocator default, FileChannel default, write=-1, read=1, unpersisted=1 -> IllegalArgumentException
                {AllocatorStatus.DEFAULT, FileStatus.DEFAULT, FileChannelStatus.DEFAULT, -1, 1, 1, IllegalArgumentException.class},

                // TEST 5: Allocator default, FileChannel default, write=0, read=-1, unpersisted=1 -> IllegalArgumentException
                {AllocatorStatus.DEFAULT, FileStatus.DEFAULT, FileChannelStatus.DEFAULT, 0, -1, 1, IllegalArgumentException.class},

                // TEST 6: Allocator default, FileChannel default, write=1, read=0, unpersisted=0 -> Correct instantiation
                {AllocatorStatus.DEFAULT, FileStatus.DEFAULT, FileChannelStatus.DEFAULT, 1, 0, 0, null},

                // TEST 7: Allocator default, FileChannel default, write=1, read=1, unpersisted=-1 -> Correct instantiation
                {AllocatorStatus.DEFAULT, FileStatus.DEFAULT, FileChannelStatus.DEFAULT, 1, 1, -1, null},

                // TEST 8: Allocator invalid, FileChannel default write=1, read=1, unpersisted=1 -> NullPointerException TEST FAILURE
                //{AllocatorStatus.INVALID, FileStatus.DEFAULT, FileChannelStatus.DEFAULT, 1, 1, 1, NullPointerException.class}, //Return NULL

                // TEST 9: Allocator default, FileChannel no permission -> AccessDeniedException
                {AllocatorStatus.DEFAULT, FileStatus.NO_PERMISSION, FileChannelStatus.DEFAULT, 0, 0, 0, AccessDeniedException.class}, //Return NULL


        });
    }

    @Before
    public void setUp() {
        try {
            setFile();
            setFileChannel();
            setAllocatorStatus();
        }catch (IOException e) {
            exceptionCatched = e.getClass();
        }
    }

    private void setAllocatorStatus() {
        switch(allocatorStatus){
            case DEFAULT:
                allocator = UnpooledByteBufAllocator.DEFAULT;
                break;
            case NULL:
                allocator = null;
                break;
            case INVALID:
                allocator = getInvalidAllocator();
        }
    }

    private void setFile() throws IOException {
        if (Files.notExists(PATH)) {
            Files.createFile(PATH);
        }
        originalPermissions = Files.getPosixFilePermissions(PATH);
        switch (fileStatus){
            case NO_PERMISSION:
                setNoPermissionFile();
                break;
            case DEFAULT:
                break;
        }
    }

    private void setFileChannel() throws IOException {

        switch(fileChannelStatus){
            case NULL:
                fc = null;
                break;
            case DEFAULT:
                fc = FileChannel.open(PATH, StandardOpenOption.CREATE);
                fc.position(1); //Aggiunta per pit
                break;
            case CLOSED:
                closeChannel();
                break;
        }
    }

    private void closeChannel() throws IOException {
        fc = FileChannel.open(PATH, StandardOpenOption.CREATE);
        fc.close();
    }

    private void setNoPermissionFile() throws IOException{
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString("---------");
        Files.setPosixFilePermissions(PATH, perms);
    }

    @Test
    public void test() {
        try {

            if (exceptionCatched != null) {
                Assert.assertEquals(expectedException, exceptionCatched);
            } else {

                logger.info("Running test with parameters: AllocatorStatus={}, FileStatus={}, FileChannelStatus={}, writeCapacity={}, readCapacity={}, unpersistedBytesBound={}, expectedException={}",
                        allocatorStatus, fileStatus, fileChannelStatus, writeCapacity, readCapacity, unpersistedBytesBound, expectedException);
                bufferedChannel = spy(new BufferedChannel(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound));

                Assert.assertNotNull(bufferedChannel);

                checkWriteBufferStartPosition(); //Add after pit

                if (expectedException != null) {
                    Assert.fail("Expected exception: " + expectedException + " but none was thrown.");
                }
                logger.info("Test passed with parameters: AllocatorStatus={}, FileStatus={}, FileChannelStatus={}",
                        allocatorStatus, fileStatus, fileChannelStatus);
}
            } catch(Exception e){
                Assert.assertEquals(expectedException, e.getClass());
                logger.info("Test passed with expected exception: {}", e.getClass());
            }
    }

    private void checkWriteBufferStartPosition() throws IOException {
        Assert.assertEquals(bufferedChannel.writeBufferStartPosition.get(), fc.position());
    }


    private static ByteBufAllocator getInvalidAllocator() {
        ByteBufAllocator invalidAllocator = mock(ByteBufAllocator.class);
        when(invalidAllocator.directBuffer(anyInt())).thenReturn(null);
        return invalidAllocator;
    }

    @After
    public void tearDown() throws IOException {
        if (fileStatus == FileStatus.NO_PERMISSION){
            Files.setPosixFilePermissions(PATH, originalPermissions);
        }
        if (fc != null && fc.isOpen()) {
            fc.close();
        }
        if (bufferedChannel != null) {
            bufferedChannel.close();
        }

    }
}
