package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;
import org.apache.bookkeeper.bookie.utils.ByteBufStatus;
import org.apache.bookkeeper.bookie.utils.FileChannelStatus;
import org.apache.bookkeeper.bookie.utils.ReadCases;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class BufferedChannelReadTest {

    int DEFAULT_CAPACITY = 1024;
    private ByteBufStatus destStatus;
    private int position;
    private int expectedResult;
    private int length;
    private Class<? extends Exception> expectedException;
    private FileChannel fc;
    private ByteBufAllocator allocator;
    private ByteBuf dest;
    private ReadCases readCases;
    private BufferedChannel bufferedChannel;
    private final Path PATH = Paths.get("src/test/java/org/apache/bookkeeper/bookie/utils/fileForTest");
    private static final Logger logger = LoggerFactory.getLogger(BufferedChannelConstructorTest.class);
    private int readCapacity;
    private int writeCapacity;
    private int unpersistedBytesBound;

    // Constructor for class parameters
    public BufferedChannelReadTest(ByteBufStatus destStatus, int position, int length, int expectedResult, int readCapacity, int writeCapacity, int unpersistedBytesBound, Class<? extends Exception> expectedException, ReadCases state) {
        logger.info("Test with parameters: Position: {}, Lenght: {}", position, length);
        this.destStatus = destStatus;
        this.position = position;
        this.expectedResult = expectedResult;
        this.expectedException = expectedException;
        this.length = length;
        this.readCases = state;
        this.unpersistedBytesBound = unpersistedBytesBound;
        this.writeCapacity = writeCapacity;
        this.readCapacity = readCapacity;
    }

    @Parameterized.Parameters()
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{


                // TEST: Dest , Pos , Length , ExpectedResult (0, if we have an Exception), ReadCapacity, WriteCapacity, UnpersistedBytesBound -> Exception

                //WriteBuffer with data
                {ByteBufStatus.NULL, 0, 1, 0, 32, 32, 32, NullPointerException.class, ReadCases.WRITE_BUF_CASE},
                {ByteBufStatus.NULL, 0, 0, 0, 32, 32, 32, null, ReadCases.WRITE_BUF_CASE},
                {ByteBufStatus.ZERO_CAPACITY, 1, 1, 0, 32, 32, 32, IOException.class, ReadCases.WRITE_BUF_CASE},
                {ByteBufStatus.ZERO_CAPACITY, -1, 1, 0, 32, 32, 32, IllegalArgumentException.class,  ReadCases.WRITE_BUF_CASE},
                {ByteBufStatus.DEFAULT, 0, -1, 0, 32, 32, 32, null,  ReadCases.WRITE_BUF_CASE},
                {ByteBufStatus.DEFAULT, 1, 20, 20, 32, 32, 32, null,  ReadCases.WRITE_BUF_CASE},

                //Read only from FC
                {ByteBufStatus.DEFAULT, 15, 1, 0, 32, 32, 32, IOException.class, ReadCases.ONLY_FC_CASE},
                {ByteBufStatus.DEFAULT, 14, 1, 0, 32, 32, 32, IOException.class, ReadCases.ONLY_FC_CASE},
                {ByteBufStatus.DEFAULT, 13, 1, 1, 32, 32, 32, null, ReadCases.ONLY_FC_CASE},


                {ByteBufStatus.INVALID, 0, 1, 0, 32, 32, 32, IllegalReferenceCountException.class, ReadCases.ONLY_FC_CASE},

                //Tests failed
                //{ByteBufStatus.DEFAULT, 1, 1, 1, 32, 32, 32, null, ReadCases.HIGH_VALUE_START_POSITION}, //Read all bytes
                //{ByteBufStatus.DEFAULT, 1, 1, 1, 32, 32, 32, null,  ReadCases.WRITE_BUF_CASE}, // Read all bytes
                //{ByteBufStatus.DEFAULT, 1, 2, 1, 32, 32, 32, null,  ReadCases.WRITE_BUF_CASE}, // Read all bytes

                //Test after JaCoCo
                {ByteBufStatus.DEFAULT, 0, 1, 0, 32, 32, 32, null, ReadCases.WRITE_BUF_NULL},
                {ByteBufStatus.DEFAULT, 6, 8, 8, 32, 32, 32, null, ReadCases.WRITE_BUF_NULL_HIGH_VALUE},

                //Test after Ba-Dua
                {ByteBufStatus.DEFAULT, 1, 20, 20, 32, 32, 32, null,  ReadCases.READ_CASE},

                //Test after pit
                {ByteBufStatus.DEFAULT, 0, 1, 0, 0, 32, 32, IOException.class, ReadCases.ONLY_FC_CASE},

        });
    }

    @Before
    public void setUp() throws IOException {
        allocator = UnpooledByteBufAllocator.DEFAULT;
        setReadCase();
        dest = setByteBufStatus(destStatus);
    }

    private void setReadCase() throws IOException {
        fc = FileChannel.open(PATH, StandardOpenOption.WRITE, StandardOpenOption.READ);

        switch (readCases){
            case WRITE_BUF_CASE:
                allocator = UnpooledByteBufAllocator.DEFAULT;
                bufferedChannel = spy(new BufferedChannel(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound));
                writePhraseToWriteBuffer(1);
                break;
            case ONLY_FC_CASE:
                allocator = UnpooledByteBufAllocator.DEFAULT;
                bufferedChannel = spy(new BufferedChannel(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound));
                bufferedChannel.writeBufferStartPosition.set(Long.MAX_VALUE);
                writePhraseToFile(1);
                break;
            case BOTH_BUF_CASE:
                allocator = UnpooledByteBufAllocator.DEFAULT;
                bufferedChannel = spy(new BufferedChannel(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound));
                writePhraseToWriteBuffer(2);
                writePhraseToFile(2);
                break;
            case WRITE_BUF_NULL:
                allocator = spy(UnpooledByteBufAllocator.DEFAULT);
                doReturn(null).when(allocator).directBuffer(anyInt());
                bufferedChannel = spy(new BufferedChannel(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound));
                break;
            case WRITE_BUF_NULL_HIGH_VALUE:
                allocator = spy(UnpooledByteBufAllocator.DEFAULT);
                doReturn(null).when(allocator).directBuffer(anyInt());
                bufferedChannel = spy(new BufferedChannel(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound));
                bufferedChannel.writeBufferStartPosition.set(Long.MAX_VALUE);
                writePhraseToFile(1);
                break;
            case READ_CASE:
                allocator = spy(UnpooledByteBufAllocator.DEFAULT);
                bufferedChannel = spy(new BufferedChannel(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound));
                bufferedChannel.writeBufferStartPosition.set(Long.MAX_VALUE);
                writeOnReadBuffer();
                break;
        }
    }

    private byte[] writePhraseToFile(int phraseType) throws IOException {
        String phrase;
        if (phraseType == 1) {
            phrase = "Sono nel file.";
        } else {
            phrase = "file.";
        }
        byte[] phraseBytes = phrase.getBytes(StandardCharsets.UTF_8);

        // Trunca il file a 0 byte prima di scrivere
        fc.truncate(0);  // <- Questo pulisce il file prima di scrivere

        ByteBuffer buffer = ByteBuffer.wrap(phraseBytes);
        while (buffer.hasRemaining()) {
            fc.write(buffer);
        }
        fc.force(true);  // Forza il write su disco
        fc.position(0);  // Reset della posizione del fileChannel
        return phraseBytes;
    }

    private byte[] writePhraseToWriteBuffer(int phraseType) {
        String phrase;
        if(phraseType == 1) {
            phrase = "Sono nel writeBuffer.";
        } else {
            phrase = "writeBuffer.";
        }
        byte[] phraseBytes = phrase.getBytes(StandardCharsets.UTF_8);

        bufferedChannel.writeBuffer.writeBytes(phraseBytes);
        return phraseBytes;
    }

    private void writeOnReadBuffer() {
        byte[] testData = "Sono nel readBuffer.".getBytes(StandardCharsets.UTF_8);
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
        logger.info("Contenuto: {}, Desiderato: {}", content, desiredContentString);
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
                    String content = new String(bytesToCheck, StandardCharsets.UTF_8);
                    logger.info("CONTENUTO: {}", content);

                    checkCorrectData(bytesToCheck);

                } else {
                    logger.info("No readable bytes");
                }

            }

        } catch (Exception e) {
            logger.info("Eccezione catturata: {}, Eccezione attesa: {}", e.getClass(), expectedException);
            Assert.assertEquals(expectedException, e.getClass());
        }
    }
}
