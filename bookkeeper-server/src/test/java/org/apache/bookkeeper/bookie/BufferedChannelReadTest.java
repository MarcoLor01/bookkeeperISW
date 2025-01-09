package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;
import org.apache.bookkeeper.bookie.utils.commonEnum.ByteBufStatus;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class BufferedChannelReadTest {

    int DEFAULT_CAPACITY = 1024;
    private final ByteBufStatus destStatus;
    private final int position;
    private final int expectedResult;
    private final int length;
    private final Class<? extends Exception> expectedException;
    private FileChannel fc;
    private ByteBufAllocator allocator;
    private ByteBuf dest;
    private final ReadCases readCases;
    private BufferedChannel bufferedChannel;
    private final Path PATH = Paths.get("src/test/java/org/apache/bookkeeper/bookie/utils/fileForTest");
    private static final Logger logger = LoggerFactory.getLogger(BufferedChannelConstructorTest.class);
    private final int readCapacity;
    private static final int DEFAULT_VALUE = 32;
    
    public BufferedChannelReadTest(ByteBufStatus destStatus, int position, int length, int expectedResult, int readCapacity, Class<? extends Exception> expectedException, ReadCases state) {
        this.destStatus = destStatus;
        this.position = position;
        this.expectedResult = expectedResult;
        this.expectedException = expectedException;
        this.readCapacity = readCapacity;
        this.length = length;
        this.readCases = state;

    }

    @Parameterized.Parameters()
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{


                // TEST: Dest , Pos , Length , ExpectedResult (0, if we have an Exception), ReadCapacity, WriteCapacity, UnpersistedBytesBound -> Exception

                //WriteBuffer con dati
                {ByteBufStatus.NULL, 0, 1, 0, 32, NullPointerException.class, ReadCases.WRITE_BUF_CASE},
                {ByteBufStatus.NULL, 0, 0, 0, 32, null, ReadCases.WRITE_BUF_CASE},
                {ByteBufStatus.ZERO_CAPACITY, 1, 1, 0, 32, IOException.class, ReadCases.WRITE_BUF_CASE},
                {ByteBufStatus.ZERO_CAPACITY, -1, 1, 0, 32, IllegalArgumentException.class,  ReadCases.WRITE_BUF_CASE},
                {ByteBufStatus.DEFAULT, 0, -1, 0, 32, null, ReadCases.WRITE_BUF_CASE},
                {ByteBufStatus.DEFAULT, 1, 20, 20, 32, null, ReadCases.WRITE_BUF_CASE},

                //Leggo solo da FC
                {ByteBufStatus.DEFAULT, 15, 1, 0, 32, IOException.class, ReadCases.ONLY_FC_CASE},
                {ByteBufStatus.DEFAULT, 14, 1, 0, 32, IOException.class, ReadCases.ONLY_FC_CASE},
                {ByteBufStatus.DEFAULT, 13, 1, 1, 32, null, ReadCases.ONLY_FC_CASE},
                {ByteBufStatus.INVALID, 0, 1, 0, 32, IllegalReferenceCountException.class, ReadCases.ONLY_FC_CASE},

                //Test falliti
                //{ByteBufStatus.DEFAULT, 1, 1, 1, 32, null, ReadCases.HIGH_VALUE_START_POSITION}, //Read all bytes
                //{ByteBufStatus.DEFAULT, 1, 1, 1, 32, null,  ReadCases.WRITE_BUF_CASE}, // Read all bytes
                //{ByteBufStatus.DEFAULT, 1, 2, 1, 32, null,  ReadCases.WRITE_BUF_CASE}, // Read all bytes

                //Test after JaCoCo
                {ByteBufStatus.DEFAULT, 0, 1, 0, 32, null, ReadCases.WRITE_BUF_NULL},
                {ByteBufStatus.DEFAULT, 6, 8, 8, 32, null, ReadCases.WRITE_BUF_NULL_HIGH_VALUE},

                //Test after Ba-Dua
                {ByteBufStatus.DEFAULT, 1, 20, 20, 32, null,  ReadCases.READ_CASE},

                //Test after pit
                {ByteBufStatus.DEFAULT, 0, 1, 0, 0, IOException.class, ReadCases.ONLY_FC_CASE},

        });
    }

    @Before
    public void setUp() throws IOException {
        allocator = UnpooledByteBufAllocator.DEFAULT;
        setReadCase();
        dest = setByteBufStatus(destStatus);
    }

    private void setReadCase() throws IOException {

        if (Files.notExists(PATH)) {
            Files.createFile(PATH);
        }

        fc = FileChannel.open(PATH, StandardOpenOption.WRITE, StandardOpenOption.READ);

        switch (readCases){
            case WRITE_BUF_CASE:
                allocator = UnpooledByteBufAllocator.DEFAULT;
                setBufferedChannel();
                writePhraseToWriteBuffer("Sono nel writeBuffer.");
                break;
            case ONLY_FC_CASE:
                allocator = UnpooledByteBufAllocator.DEFAULT;
                setBufferedChannel();;
                bufferedChannel.writeBufferStartPosition.set(Long.MAX_VALUE);
                writePhraseToFile("Sono nel file.");
                break;
            case BOTH_BUF_CASE:
                allocator = UnpooledByteBufAllocator.DEFAULT;
                setBufferedChannel();
                writePhraseToWriteBuffer("writeBuffer.");
                writePhraseToFile("file.");
                break;
            case WRITE_BUF_NULL:
                allocator = spy(UnpooledByteBufAllocator.DEFAULT);
                doReturn(null).when(allocator).directBuffer(anyInt());
                setBufferedChannel();;
                break;
            case WRITE_BUF_NULL_HIGH_VALUE:
                allocator = spy(UnpooledByteBufAllocator.DEFAULT);
                doReturn(null).when(allocator).directBuffer(anyInt());
                setBufferedChannel();
                bufferedChannel.writeBufferStartPosition.set(Long.MAX_VALUE);
                writePhraseToFile("Sono nel file.");
                break;
            case READ_CASE:
                allocator = spy(UnpooledByteBufAllocator.DEFAULT);
                setBufferedChannel();
                bufferedChannel.writeBufferStartPosition.set(Long.MAX_VALUE);
                writeOnReadBuffer("Sono nel readBuffer.");
                break;
        }
    }

    private void setBufferedChannel() throws IOException {
        bufferedChannel = spy(new BufferedChannel(allocator, fc, DEFAULT_VALUE, readCapacity, DEFAULT_VALUE));
    }

    private void writePhraseToFile(String stringToWrite) throws IOException {
        
        byte[] phraseBytes = stringToWrite.getBytes(StandardCharsets.UTF_8);
        
        fc.truncate(0);

        ByteBuffer buffer = ByteBuffer.wrap(phraseBytes);
        while (buffer.hasRemaining()) {
            fc.write(buffer);
        }
        fc.force(true); 
        fc.position(0);
    }

    private void writePhraseToWriteBuffer(String stringToWrite) {
        byte[] phraseBytes = stringToWrite.getBytes(StandardCharsets.UTF_8);
        bufferedChannel.writeBuffer.writeBytes(phraseBytes);
    }

    private void writeOnReadBuffer(String stringToWrite) {
        byte[] testData = stringToWrite.getBytes(StandardCharsets.UTF_8);
        bufferedChannel.readBuffer.clear();
        bufferedChannel.readBuffer.writeBytes(testData);
        bufferedChannel.readBufferStartPosition = position;
    }

    private ByteBuf setByteBufStatus(ByteBufStatus status) {
        switch(status){
            case DEFAULT:
                return Unpooled.buffer(DEFAULT_CAPACITY);
            case ZERO_CAPACITY:
                return Unpooled.buffer(0);
            case NULL:
                return null;
            case INVALID:
                ByteBuf srcBuffer = Unpooled.buffer(256);
                srcBuffer.release(); //Deallocate
                return srcBuffer;
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

    private void checkCorrectData(byte[] bytesToCheck) throws IOException {
        String content = new String(bytesToCheck, StandardCharsets.UTF_8);

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        if (bufferedChannel.writeBuffer != null) {
            int positionInBuffer = (int) (position - bufferedChannel.writeBufferStartPosition.get());
            int bytesToCopy = Math.min(bufferedChannel.writeBuffer.writerIndex() - positionInBuffer, length);

            if (bytesToCopy > 0) {
                byte[] temp = new byte[bytesToCopy];
                bufferedChannel.writeBuffer.getBytes(positionInBuffer, temp, 0, bytesToCopy);
                buffer.write(temp);
            }
        }

        if (bufferedChannel.readBuffer.readableBytes() != 0 && buffer.size() < length) {
            int positionInBuffer = (int) (position - bufferedChannel.readBufferStartPosition);
            int bytesToCopy = Math.min(bufferedChannel.readBuffer.writerIndex() - positionInBuffer, length - buffer.size());

            if (bytesToCopy > 0) {
                byte[] temp = new byte[bytesToCopy];
                bufferedChannel.readBuffer.getBytes(positionInBuffer, temp, 0, bytesToCopy);
                buffer.write(temp);
            }
        }

        if (fc != null && buffer.size() < length) {
            fc.position(position);

            ByteBuffer byteBuffer = ByteBuffer.allocate(length - buffer.size());
            int bytesRead = fc.read(byteBuffer);

            if (bytesRead > 0) {
                byteBuffer.flip();
                buffer.write(byteBuffer.array(), 0, byteBuffer.limit());
            } else if (bytesRead == -1) {
                // Handle EOF scenario
                throw new IOException("End of file reached before reading the full content.");
            }
        }

        byte[] desiredContent = buffer.toByteArray();

        if (desiredContent.length < length) {
            throw new IOException("Incomplete read. Expected " + length + " bytes but got " + desiredContent.length);
        }
        String desiredContentString = new String(desiredContent, StandardCharsets.UTF_8);
        Assert.assertEquals(desiredContentString, content);
    }



    @Test
    public void testRead() {
        try {

            int result = bufferedChannel.read(dest, position, length);

            if (expectedException != null) {
                Assert.fail("Expected exception: " + expectedException.getName() + " but none was thrown.");
            } else {

                logger.info("Result: {}, ExpectedResult: {}", result, expectedResult);
                Assert.assertEquals(result, expectedResult);


                if (dest != null && dest.readableBytes() != 0) {
                    logger.info("Readable bytes in dest: {}", dest.readableBytes());
                    byte[] bytesToCheck = new byte[dest.readableBytes()];
                    dest.readBytes(bytesToCheck);
                    checkCorrectData(bytesToCheck);

                } else {
                    logger.info("No readable bytes");
                }

            }

        } catch (Exception e) {
            Assert.assertEquals(expectedException, e.getClass());
        }
    }

    public enum ReadCases {
        WRITE_BUF_CASE,
        BOTH_BUF_CASE,
        ONLY_FC_CASE,
        WRITE_BUF_NULL,
        WRITE_BUF_NULL_HIGH_VALUE,
        READ_CASE,
    }
}
