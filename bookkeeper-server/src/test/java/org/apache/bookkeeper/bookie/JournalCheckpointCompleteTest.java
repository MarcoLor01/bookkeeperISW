package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.bookkeeper.bookie.BookieUtilJournal.writeV4Journal;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

@RunWith(Parameterized.class)
public class JournalCheckpointCompleteTest {
    private final CheckPointStatus checkPointStatus;
    private Checkpoint checkpoint;
    private final boolean compact;
    private final byte[] KEY = "test".getBytes();
    private final Class<? extends Exception> expectedException;
    private BookieImpl bookie;
    private Journal journal;
    private final boolean maxJournals;
    private final JournalNumber journalNumber;
    private List<String> ledgerDirNames;

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                // TEST: checkpoint, compact -> Expected
                {CheckPointStatus.VALID, true, null, false, JournalNumber.DEFAULT},
                {CheckPointStatus.INVALID, true, null, false, JournalNumber.DEFAULT},
                {CheckPointStatus.NULL, true, null, false, JournalNumber.DEFAULT},
                {CheckPointStatus.VALID, false, null, false, JournalNumber.DEFAULT},
                {CheckPointStatus.INVALID, false, null, false, JournalNumber.DEFAULT},
                {CheckPointStatus.NULL, false, null, false, JournalNumber.DEFAULT},
                //After JaCoCo, non tocchiamo fin'ora il if (logs.size() >= maxBackupJournals), introduciamo nuova variabile
                {CheckPointStatus.VALID, true, null, true, JournalNumber.DEFAULT},
                //After ba-dua, i=MAX_BACKUP
                {CheckPointStatus.VALID, true, null, true, JournalNumber.MAX_BACKUP}

        });
    }

    @Before
    public void setUp() throws Exception {
        File journalDirectory = journalSetting();

        setCheckpoint();

        if (maxJournals){
            if(journalNumber == JournalNumber.MAX_BACKUP){
                for (int i=0; i<4;i++){
                    writeV4Journal(journalDirectory, 50, KEY); //MAX_BACKUP è pari a 5, ma noi effettuiamo già una scrittura precedente
                }
            } else {
                for (int i = 0; i < 15; i++) {
                    writeV4Journal(journalDirectory, 50, KEY);
                }
            }
        }

    }

    public File journalSetting() throws Exception {
        File journalDirectory = createTempDirectory("bookie", "journal");
        File ledgerDirectory = createTempDirectory("bookie", "ledger");
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDirectory));
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDirectory));
        writeV4Journal(BookieImpl.getCurrentDirectory(journalDirectory), 5, KEY);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf
                .setJournalDirsName(new String[] {journalDirectory.getPath()})
                .setLedgerDirNames(new String[] { ledgerDirectory.getPath() })
                .setMetadataServiceUri(null);

        String[] tempList = new String[conf.getLedgerDirNames().length];
        for (int i = 0; i < conf.getLedgerDirNames().length; i++) {
            tempList[0] = BookieImpl.getCurrentDirectory(new File(conf.getLedgerDirNames()[0])).toString();
        }
        ledgerDirNames = Arrays.asList(tempList);
        bookie = new TestBookieImpl(conf);
        journal = bookie.journals.get(0);
        return BookieImpl.getCurrentDirectory(journalDirectory);
    }

    public JournalCheckpointCompleteTest(CheckPointStatus checkPointStatus, boolean compact,
                                  Class<? extends Exception> expectedException, boolean maxJournals, JournalNumber journalNumber){
        this.checkPointStatus = checkPointStatus;
        this.compact = compact;
        this.expectedException = expectedException;
        this.maxJournals = maxJournals;
        this.journalNumber = journalNumber;
    }

    private void setCheckpoint(){
        switch (checkPointStatus){
            case NULL:
                checkpoint = null;
                break;
            case VALID:

                //After Jacoco
                if (maxJournals){
                    journal.setLastLogMark(2000000000000L, 0L);
                }

                checkpoint = spy(this.journal.newCheckpoint());
                break;
            case INVALID:
                checkpoint = CheckpointSource.DEFAULT.newCheckpoint(); //Checkpoint non di questo Journal
                break;

        }
    }

    @Test
    public void test() {
        try {
            Assert.assertNotNull(journal);
            journal.checkpointComplete(checkpoint, compact);

            //After pit, testiamo side effects
            if (checkPointStatus == CheckPointStatus.VALID) {

                List<File> writableLedgerDirs = journal.getLedgerDirsManager()
                        .getWritableLedgerDirsForNewLog();

                List<String> writableLedgerDirPaths = writableLedgerDirs.stream()
                        .map(File::getAbsolutePath)
                        .collect(Collectors.toList());

                Assert.assertEquals(writableLedgerDirPaths, ledgerDirNames);

                for (File dir : writableLedgerDirs) {
                    File expectedFile = new File(dir, journal.getLastMarkFileName());
                    assertTrue("File not created", expectedFile.exists());
                }
            }

        if (expectedException != null) {
                Assert.fail("Expected exception: " + expectedException + " but none was thrown.");
            }

        } catch(Exception e){
            Assert.assertEquals(expectedException, e.getClass());
        }
    }

    @After
    public void cleanUp() {
        if (bookie != null) {
            this.bookie.shutdown();
        }
        for (File dir : tempDirs) {
            FileUtils.deleteQuietly(dir);
        }
        tempDirs.clear();
    }

        List<File> tempDirs = new ArrayList<>();

    File createTempDirectory(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }

    public enum CheckPointStatus {
        VALID,
        INVALID,
        NULL,
    }

    public enum JournalNumber {
        DEFAULT,
        MAX_BACKUP,
    }

}
