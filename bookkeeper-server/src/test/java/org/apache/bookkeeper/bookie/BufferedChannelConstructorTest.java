package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.utils.commonEnum.AllocatorStatus;
import org.apache.bookkeeper.bookie.utils.commonEnum.FileChannelStatus;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class BufferedChannelConstructorTest {

    private BufferedChannel bufferedChannel;
    private ByteBufAllocator allocator;
    private final FileStatus fileStatus;
    private FileChannel fc;
    private final FileChannelStatus fileChannelStatus;
    private final Class<? extends Exception> expectedException;
    private final int writeCapacity;
    private final int readCapacity;
    private final long unpersistedBytesBound;
    private final AllocatorStatus allocatorStatus;
    private static final Logger logger = LoggerFactory.getLogger(BufferedChannelConstructorTest.class);
    private Class<? extends Exception> exceptionCatched;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    private Path PATH;


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

                // TEST 1: Allocator null, FileChannel default, write, read, unpersisted -> Exception
                {AllocatorStatus.NULL, FileStatus.DEFAULT, FileChannelStatus.DEFAULT, 0, 0, 0, NullPointerException.class},
                {AllocatorStatus.DEFAULT, FileStatus.DEFAULT, FileChannelStatus.NULL, 1, 0, 0, NullPointerException.class},
                {AllocatorStatus.DEFAULT, FileStatus.DEFAULT, FileChannelStatus.CLOSED, 1, 1, 1, ClosedChannelException.class},
                {AllocatorStatus.DEFAULT, FileStatus.DEFAULT, FileChannelStatus.DEFAULT, -1, 1, 1, IllegalArgumentException.class},
                {AllocatorStatus.DEFAULT, FileStatus.DEFAULT, FileChannelStatus.DEFAULT, 0, -1, 1, IllegalArgumentException.class},
                {AllocatorStatus.DEFAULT, FileStatus.DEFAULT, FileChannelStatus.DEFAULT, 1, 0, 0, null},
                {AllocatorStatus.DEFAULT, FileStatus.DEFAULT, FileChannelStatus.DEFAULT, 1, 1, -1, null},
                //{AllocatorStatus.INVALID, FileStatus.DEFAULT, FileChannelStatus.DEFAULT, 1, 1, 1, NullPointerException.class}, //Return NULL
                {AllocatorStatus.DEFAULT, FileStatus.NO_PERMISSION, FileChannelStatus.DEFAULT, 0, 0, 0, AccessDeniedException.class},

        });
    }

    @Before
    public void setUp() throws IOException {
        PATH = tempFolder.newFile().toPath();
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
        Set<PosixFilePermission> defaultPerms = PosixFilePermissions.fromString("rw-r--r--");
        Files.setPosixFilePermissions(PATH, defaultPerms);

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
                fc = FileChannel.open(PATH, StandardOpenOption.CREATE, StandardOpenOption.READ);
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

        Path parent = PATH.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
    }

    @Test
    public void test() {
        try {

            if (exceptionCatched != null) {
                Assert.assertEquals(expectedException, exceptionCatched);
            } else {
                bufferedChannel = new BufferedChannel(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound);
                Assert.assertNotNull(bufferedChannel);
                Assert.assertEquals(bufferedChannel.writeBuffer.capacity(), writeCapacity);

                checkWriteBufferStartPosition(); //Add after pit

                if (expectedException != null) {
                    Assert.fail("Expected exception: " + expectedException + " but none was thrown.");
                }
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
    public void tearDown() {

        try {
            if (bufferedChannel != null) {
                bufferedChannel.close();
            }
        } catch (IOException e) {
            logger.error("Error closing BufferedChannel", e);
        }

        try {
            if (fc != null && fc.isOpen()) {
                fc.close();
            }
        } catch (IOException e) {
            logger.error("Error closing FileChannel", e);
        }
    }

    public enum FileStatus {
        NO_PERMISSION,
        DEFAULT,
    }

}
