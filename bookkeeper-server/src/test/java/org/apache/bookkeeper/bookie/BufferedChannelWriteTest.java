package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.utils.commonEnum.ByteBufStatus;
import org.apache.bookkeeper.bookie.utils.commonEnum.FileChannelStatus;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class BufferedChannelWriteTest {

    int DEFAULT_CAPACITY = 1024;
    private BufferedChannel bufferedChannel;
    private ByteBufAllocator allocator;
    private FileChannel fc;
    private ByteBuf src;
    private final ByteBufStatus srcStatus;
    private final int writeCapacity;
    private final Class<? extends Exception> expectedException;
    private final FileChannelStatus fileChannelStatus;
    private final Path PATH = Paths.get("src/test/java/org/apache/bookkeeper/bookie/utils/fileForTest");
    private final int bytesToWriteInSrc = DEFAULT_CAPACITY / 4;
    private final int unpersistedBytesBound;
    private byte[] randomBytes;


    // Constructor for class parameters
    public BufferedChannelWriteTest(ByteBufStatus srcStatus, int writeCapacity, FileChannelStatus fileChannelStatus, int unpersistedBytesBound, Class<? extends Exception> expectedException) {
        this.srcStatus = srcStatus;
        this.writeCapacity = writeCapacity;
        this.expectedException = expectedException;
        this.fileChannelStatus = fileChannelStatus;
        this.unpersistedBytesBound = unpersistedBytesBound;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                // TEST 1: Write with `src` null, writeBuffer valid -> NullPointerException
                {ByteBufStatus.NULL, 512, FileChannelStatus.DEFAULT, 256, NullPointerException.class},

                // TEST 2: Valid write on buffer, writeBuffer valid -> No exception
                {ByteBufStatus.DEFAULT, 256, FileChannelStatus.DEFAULT, 256, null},

                // TEST 3: Write with capacity 0 -> No exception
                {ByteBufStatus.ZERO_CAPACITY, 512, FileChannelStatus.DEFAULT, 0, null},

                // TEST 4: `file channel` read only -> NonWritableChannelException
                {ByteBufStatus.DEFAULT, 512, FileChannelStatus.READ_ONLY, 256, NonWritableChannelException.class},

                //TEST 5: channel closed -> ClosedChannelException
                {ByteBufStatus.DEFAULT, 512, FileChannelStatus.CLOSED, 256, ClosedChannelException.class},

                //TEST 6: ByteBuf INVALID
                //{ByteBufStatus.INVALID, 256, FileChannelStatus.DEFAULT, 256, IllegalReferenceCountException.class},

                //Add after jacoco, need to go in the internal if
                {ByteBufStatus.DEFAULT, 512, FileChannelStatus.DEFAULT, 320, null},

                //Add after Ba-Dua
                {ByteBufStatus.EMPTY, 512, FileChannelStatus.DEFAULT, 320, null},
                {ByteBufStatus.BIG, 128, FileChannelStatus.DEFAULT, 36, null},
        });
    }

    @Before
    public void setUp() throws IOException {
        allocator = UnpooledByteBufAllocator.DEFAULT;
        setFileChannel();
        src = setByteBufStatus(srcStatus);
        if(src != null && srcStatus != ByteBufStatus.INVALID && srcStatus != ByteBufStatus.EMPTY) {
            randomBytes = writeRandomByte();
        }
    }

    private byte[] writeRandomByte(){
        byte[] randomBytes = new byte[bytesToWriteInSrc];
        new Random().nextBytes(randomBytes);
        src.clear();
        src.writeBytes(randomBytes);
        return randomBytes;
    }

    private void closeChannel() throws IOException {
        fc = FileChannel.open(PATH, StandardOpenOption.CREATE);
        fc.close();
    }

    private void setFileChannel() throws IOException {
        if (Files.notExists(PATH)) {
            Files.createFile(PATH);
        }
        switch(fileChannelStatus){
            case NULL:
                fc = null;
                break;
            case DEFAULT:
                fc = FileChannel.open(PATH, StandardOpenOption.WRITE);
                break;
            case CLOSED:
                closeChannel();
                break;
            case READ_ONLY:
                fc = FileChannel.open(PATH, StandardOpenOption.READ);
        }
    }

    private ByteBuf setByteBufStatus(ByteBufStatus status) {
        switch(status){
            case DEFAULT:
            case EMPTY:
                return Unpooled.buffer(DEFAULT_CAPACITY);
            case ZERO_CAPACITY:
                return Unpooled.buffer(0);
            case NULL:
                return null;
            case INVALID:
                ByteBuf srcBuffer = Unpooled.buffer(256);
                srcBuffer.release(); //Deallocate
                return srcBuffer;
            case BIG:
                return Unpooled.buffer(8192);
        }
        return null;
    }


    @After
    public void tearDown() throws IOException {
        if (Files.exists(PATH)) {
            Files.delete(PATH);
        }
        if (fc != null && fc.isOpen()) {
            fc.close();
        }

        if (bufferedChannel != null) {
            bufferedChannel.close();
        }
    }


    @Test
    public void testWrite() {
        try {
            long initialPosition = fc.position();
            if(unpersistedBytesBound == 0) {
                bufferedChannel = spy(new BufferedChannel(allocator, fc, writeCapacity));
            } else {
                bufferedChannel = spy(new BufferedChannel(allocator, fc, writeCapacity, 256, unpersistedBytesBound));
            }
            bufferedChannel.write(src);

            long finalPosition = fc.position();
            if (expectedException != null) {
                Assert.fail("Expected exception: " + expectedException.getName() + " but none was thrown.");
            }

            if (srcStatus == ByteBufStatus.ZERO_CAPACITY) {
                Assert.assertEquals(finalPosition, initialPosition);
            } else if (unpersistedBytesBound > 0 && bytesToWriteInSrc < unpersistedBytesBound) {
                Assert.assertEquals(finalPosition, initialPosition); //No flush
            } else {
                byte[] bytesToCheck = new byte[src.readableBytes()];
                src.getBytes(0, bytesToCheck);
                Assert.assertArrayEquals(bytesToCheck, randomBytes);
                //Add after pit
                verify(bufferedChannel, atLeastOnce()).flush();
            }
            //Add after pit
            if(unpersistedBytesBound <= bytesToWriteInSrc && unpersistedBytesBound != 0){
                verify(bufferedChannel, atLeastOnce()).forceWrite(false);
            }

        } catch (Exception e) {
            Assert.assertEquals(expectedException, e.getClass());
        }
    }
}

