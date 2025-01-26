package org.apache.bookkeeper.bookie;

import lombok.Getter;
import org.apache.bookkeeper.bookie.Journal.JournalScanner;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.bookkeeper.bookie.BookieUtilJournal.writeV4Journal;
import static org.apache.bookkeeper.bookie.BookieUtilJournal.writeV5Journal;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class JournalScanJournalTest {
    private long journalId;
    private final long journalPos;
    private final ScannerStatus scannerStatus;
    private final boolean skipInvalidRecord;
    private final Class<? extends Exception> expectedException;
    private JournalScanner journalScanner;
    private Journal journal;
    private long bytesWritten;
    private static final long EMPTY_JOURNAL = 516;
    private BookieImpl bookie;
    private final byte[] KEY = "test".getBytes();
    private static final int NUM_ENTRY = 10;
    private final Version version;
    private final boolean negativeValue;
    private static final int MAX_VALUE = 67000;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();


    public JournalScanJournalTest(long journalId, long journalPos, ScannerStatus scanner, boolean skipInvalidRecord, Version version, boolean negativeValue,
                                  Class<? extends Exception> expectedException){
        this.journalId = journalId;
        this.journalPos = journalPos;
        this.scannerStatus = scanner;
        this.skipInvalidRecord = skipInvalidRecord;
        this.version = version;
        this.negativeValue = negativeValue;
        this.expectedException = expectedException;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{

                // TEST: JournalId, journalPos, scanner, skipInvalidRecord, Version (After Jacoco), Negative Value (After Jacoco) -> Expected
                {-1, 0, ScannerStatus.VALID, true, Version.VERSION_4, false, null},
                {1, 0, ScannerStatus.VALID, false, Version.VERSION_4, false, null},
                {1, -1, ScannerStatus.VALID, true, Version.VERSION_4, false, null},
                {0, -1, ScannerStatus.VALID, true, Version.VERSION_4, false, null},
                {1, 1, ScannerStatus.VALID, true, Version.VERSION_4, false, null},
                {1, Long.MAX_VALUE, ScannerStatus.VALID, true, Version.VERSION_4, false, null},
                {0, 0, ScannerStatus.NULL, false, Version.VERSION_4, false, NullPointerException.class},
                {0, 0, ScannerStatus.INVALID, false, Version.VERSION_4, false, IOException.class},
                {0, 0, ScannerStatus.INVALID, true, Version.VERSION_4, false, null},
                {1, 0, ScannerStatus.VALID, false, Version.VERSION_4, false, null},
                {0, 1, ScannerStatus.VALID, false, Version.VERSION_4, false, null},

                //After JaCoCo
                {-1, 0, ScannerStatus.VALID, true, Version.VERSION_5, false, null},
                {-1, 0, ScannerStatus.VALID, false, Version.VERSION_5_CORRUPTED, true, IOException.class},
                {-1, 0, ScannerStatus.VALID, true, Version.VERSION_5_CORRUPTED, true, null},

                //After Ba-Dua
                {0, 0, ScannerStatus.VALID, false, Version.VERSION_4_LEN_MAX, true, null},
        });
    }

    @Before
    public void setUp() throws Exception {
        setScanner();

        File journalDirectory = createTempDirectory("bookie", "journal");
        File ledgerDirectory = createTempDirectory("bookie", "ledger");

        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDirectory));
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDirectory));

        if(version == Version.VERSION_4){
            if (!negativeValue) {
                try (JournalChannel journalChannel = writeV4Journal(BookieImpl.getCurrentDirectory(journalDirectory), NUM_ENTRY, KEY)) {
                    bytesWritten += journalChannel.fc.position() + Integer.BYTES; //Tipo di Journal
                }
            } else {
                try (JournalChannel journalChannel = writeV4Journal(BookieImpl.getCurrentDirectory(journalDirectory), NUM_ENTRY, KEY, -1)) {
                    bytesWritten += journalChannel.fc.position() + Integer.BYTES; //Tipo di Journal
                }
            }
        }

        if (version == Version.VERSION_4_LEN_MAX){
            try (JournalChannel journalChannel = writeV4Journal(BookieImpl.getCurrentDirectory(journalDirectory), NUM_ENTRY, KEY, MAX_VALUE)) {
                bytesWritten += journalChannel.fc.position() + Integer.BYTES; //Tipo di Journal
            }
        }

        if(version == Version.VERSION_5){
            try (JournalChannel journalChannel = writeV5Journal(BookieImpl.getCurrentDirectory(journalDirectory), NUM_ENTRY, KEY)) {
                bytesWritten += journalChannel.fc.position() + Integer.BYTES; //Tipo di Journal
            }
        }

        if (version == Version.VERSION_5_CORRUPTED){
            try (JournalChannel journalChannel = writeV5Journal(BookieImpl.getCurrentDirectory(journalDirectory), NUM_ENTRY, KEY, true)) {
                bytesWritten += journalChannel.fc.position() + Integer.BYTES; //Tipo di Journal
            }
        }



        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf
                .setJournalDirsName(new String[] {journalDirectory.getPath()})
                .setLedgerDirNames(new String[] { ledgerDirectory.getPath() })
                .setMetadataServiceUri(null);

        bookie = new TestBookieImpl(conf);
        this.journal = bookie.journals.get(0);

        if(journalId != 1){
            journalId = Journal.listJournalIds(this.journal.getJournalDirectory(), null).get(0);
        }
    }



    private File createTempDirectory(String prefix, String suffix) throws IOException {
        return tempFolder.newFolder(prefix + "_" + suffix);
    }


    private void setScanner() throws IOException{
        switch (scannerStatus) {
            case NULL:
                journalScanner = null;
                break;
            case VALID:
                journalScanner = new ValidJournalScan();
                break;
            case INVALID:
                journalScanner = getInvalidJournalScanner();
                break;
        }
    }

    @Test
    public void test() {
        try {
            long bytesRead = journal.scanJournal(journalId, journalPos, journalScanner, skipInvalidRecord);
            Assert.assertNotNull(journal);

            if (expectedException != null) {
                Assert.fail("Expected exception: " + expectedException + " but none was thrown.");
            }
            if(scannerStatus == ScannerStatus.INVALID && journalId != 1){
                Assert.assertEquals(KEY.length + 32, bytesRead);
            } else {
                if (version == Version.VERSION_5_CORRUPTED && skipInvalidRecord) {
                    Assert.assertEquals(516, bytesRead);
                } else {
                    if (journalPos <= 0) {
                        if (journalId != 1) {
                            if (version == Version.VERSION_4_LEN_MAX){
                                Assert.assertEquals(MAX_VALUE + 16, bytesRead);
                            } else {
                                Assert.assertEquals(bytesWritten, bytesRead); //Se l'ID è 0 leggo Journal corretto
                            }
                            if (journalScanner instanceof ValidJournalScan && version != Version.VERSION_4_LEN_MAX) {
                                Assert.assertEquals(NUM_ENTRY + 2, ((ValidJournalScan) journalScanner).getProcessCount()); //Num entry + primo mex + fence mex
                            } else if (journalScanner instanceof ValidJournalScan) {
                                Assert.assertEquals(1, ((ValidJournalScan) journalScanner).getProcessCount()); //Arriva solo una volta nell'if
                            }
                        } else {
                            Assert.assertEquals(EMPTY_JOURNAL, bytesRead); //Leggo Journal vuoto
                            Assert.assertEquals(((ValidJournalScan) journalScanner).getProcessCount(), 0);
                        }
                    } else {
                        if (journalId != 1) {
                            Assert.assertTrue(bytesWritten != bytesRead);
                            Assert.assertEquals(((ValidJournalScan) journalScanner).getProcessCount(), 0);
                        } else {
                            Assert.assertEquals(EMPTY_JOURNAL, bytesRead); //Leggo Journal vuoto
                            Assert.assertEquals(((ValidJournalScan) journalScanner).getProcessCount(), 0);
                        }
                    }
                }
            }
        } catch(Exception e){
            Assert.assertEquals(expectedException, e.getClass());
        }
    }

    @After
    public void cleanUp() {
        if (bookie != null) {
            bookie.shutdown();
        }
    }

    public enum ScannerStatus {
        NULL,
        VALID,
        INVALID,
    }

    @Getter
    private static class ValidJournalScan implements JournalScanner {
        private int processCount = 0;

        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) {
            processCount++;
        }

    }

    private JournalScanner getInvalidJournalScanner() throws IOException {
        JournalScanner scanner = mock(JournalScanner.class);
        doThrow(new IOException("Simulated scan error"))
                .when(scanner).process(anyInt(), anyLong(), any(ByteBuffer.class));
        return scanner;
    }

    public enum Version{
        VERSION_4,
        VERSION_4_LEN_MAX,
        VERSION_5,
        VERSION_5_CORRUPTED,
    }


}
