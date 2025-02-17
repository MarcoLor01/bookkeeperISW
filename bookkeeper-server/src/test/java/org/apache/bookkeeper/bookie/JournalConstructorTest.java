package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.utils.InvalidLedgerDirsManager;
import org.apache.bookkeeper.bookie.utils.InvalidStatsLogger;
import org.apache.bookkeeper.bookie.utils.commonEnum.AllocatorStatus;
import org.apache.bookkeeper.common.collections.BatchedArrayBlockingQueue;
import org.apache.bookkeeper.common.collections.BlockingMpscQueue;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
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
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.apache.bookkeeper.bookie.Journal.KB;
import static org.apache.bookkeeper.bookie.Journal.MB;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class JournalConstructorTest {

    private final int journalIndex;
    private final JournalDirectoryStatus journalDirectoryStatus;
    private final ConfigurationStatus configurationStatus;
    private final LedgerDirsManagerStatus ledgerDirsManagerStatus;
    private final StatsLoggerStatus statsLoggerStatus;
    private final AllocatorStatus allocatorStatus;
    private File journalDirectory;
    private ServerConfiguration configuration;
    private StatsLogger statsLogger;
    private ByteBufAllocator byteBufAllocator;
    private LedgerDirsManager ledgerDirsManager;
    private final Class<? extends Exception> expectedException;
    private final boolean isBusyWait;
    private final boolean notExistentFileChannelProvider;
    private final boolean moreThanOneJournal;
    private final boolean flushWhenEmpty;

    public JournalConstructorTest(int journalIndex, JournalDirectoryStatus journalDirectoryStatus, ConfigurationStatus configurationStatus,
                                  LedgerDirsManagerStatus ledgerDirsManagerStatus, StatsLoggerStatus statsLoggerStatus,
                                  AllocatorStatus allocatorStatus,  boolean isBusyWait, boolean notExistentFileChannelProvider, boolean moreThanOneJournal,
                                  boolean flushWhenEmpty, Class<? extends Exception> expectedException){
        this.journalIndex = journalIndex;
        this.journalDirectoryStatus = journalDirectoryStatus;
        this.configurationStatus = configurationStatus;
        this.ledgerDirsManagerStatus = ledgerDirsManagerStatus;
        this.statsLoggerStatus = statsLoggerStatus;
        this.allocatorStatus = allocatorStatus;
        this.isBusyWait = isBusyWait;
        this.notExistentFileChannelProvider = notExistentFileChannelProvider;
        this.expectedException = expectedException;
        this.moreThanOneJournal = moreThanOneJournal;
        this.flushWhenEmpty = flushWhenEmpty;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                // TEST: journalIndex, journalDirectory, conf, ledgerDirsManager, statsLogger, allocator, isBusy (after jacoco), NotExistentChannelProvider (after jacoco) -> Expected
                {0, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, false, false, false, false, null},
                {-1, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, false, false, false, false, null},
                {1, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, false, false, false, false, null},
                {0, JournalDirectoryStatus.NULL, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, false, false, false, false, null},
                {0, JournalDirectoryStatus.VALID, ConfigurationStatus.NULL, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, false, false, false, false, NullPointerException.class},
                {1, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.NULL, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, false, false, false, false, NullPointerException.class},
                {0, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.NULL, AllocatorStatus.DEFAULT, false, false, false, false, NullPointerException.class},
                {1, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.NULL, false, false, false, false, null},
                {1, JournalDirectoryStatus.INVALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, false, false, false, false, null},
                {0, JournalDirectoryStatus.VALID, ConfigurationStatus.INVALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, false, false, false, false, IllegalStateException.class},
                {0, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.INVALID, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, false, false, false, false, IllegalStateException.class},
                {0, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.INVALID, AllocatorStatus.DEFAULT, false, false, false, false, NullPointerException.class},
                {0, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.INVALID, false, false, false, false, null},

                //Add after Jacoco
                {0, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, true, false, false, true, null},
                {0, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, true, true, true, false, RuntimeException.class},
        });
    }

    @Before
    public void setUp() throws IOException {
        setDirectory();
        setConf();
        setStatsLogger();
        setAllocator();
        try {
            setLedgerDirsManager();
        } catch (Exception e) {
            Assert.assertEquals(e.getClass(), expectedException);
        }
    }

    private void setAllocator() {
        switch (allocatorStatus){
            case NULL:
                byteBufAllocator = null;
                break;
            case INVALID:
                byteBufAllocator = getInvalidAllocator();
                break;
            case DEFAULT:
                byteBufAllocator = UnpooledByteBufAllocator.DEFAULT;
                break;
        }
    }

    private static ByteBufAllocator getInvalidAllocator() {
        ByteBufAllocator invalidAllocator = mock(ByteBufAllocator.class);
        when(invalidAllocator.directBuffer(anyInt())).thenReturn(null);
        return invalidAllocator;
    }

    private void setStatsLogger() {
        switch (statsLoggerStatus) {
            case NULL:
                statsLogger = null;
                break;
            case VALID:
                statsLogger = new NullStatsLogger();
                break;
            case INVALID:
                statsLogger = new InvalidStatsLogger();
                break;
        }
    }



    private void setLedgerDirsManager() throws IOException {
        switch (ledgerDirsManagerStatus){
            case NULL:
                ledgerDirsManager = null;
                break;
            case VALID:
                ledgerDirsManager = new LedgerDirsManager(configuration, configuration.getLedgerDirs(), new DiskChecker(0.99f, 0.98f));
                break;
            case INVALID:
                ledgerDirsManager = new InvalidLedgerDirsManager(configuration, configuration.getLedgerDirs(), new DiskChecker(0.99f, 0.98f));
                break;
        }

    }

    private void setConf() throws IOException {
        switch (configurationStatus) {
            case NULL:
                configuration = null;
                break;
            case VALID:
                configuration = TestBKConfiguration.newServerConfiguration();

                File journalDir = IOUtils.createTempDir("bookie", "journal");
                File ledgerDir = IOUtils.createTempDir("bookie", "ledger");

                configuration.setJournalDirName(journalDir.getAbsolutePath());
                configuration.setLedgerDirNames(new String[]{ledgerDir.getAbsolutePath()});
                if (isBusyWait || notExistentFileChannelProvider || moreThanOneJournal) {
                    configuration = spy(configuration);
                    if (isBusyWait) {
                        doReturn(true).when(configuration).isBusyWaitEnabled();
                    }
                    if (notExistentFileChannelProvider){
                        doReturn("notExistentClass").when(configuration).getJournalChannelProvider();
                    }
                    if (moreThanOneJournal){
                        File[] mockJournalDirs = {new File("dir1"), new File("dir2")};
                        doReturn(mockJournalDirs).when(configuration).getJournalDirs();
                    }
                }

                if (flushWhenEmpty){
                    configuration.setJournalFlushWhenQueueEmpty(false);
                }

                break;
            case INVALID:
                configuration = new InvalidServerConfiguration();
                break;
        }
    }



    public static class InvalidServerConfiguration extends ServerConfiguration {

        @Override
        public boolean getJournalSyncData() {
            throw new IllegalStateException("Invalid journal sync data configuration");
        }

    }

    private void setDirectory() throws IOException {
        switch (journalDirectoryStatus){
            case NULL:
                journalDirectory = null;
                break;
            case VALID:
                journalDirectory = IOUtils.createTempDir("bookie", "journal");
                BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDirectory));
                break;
            case INVALID:
                journalDirectory = IOUtils.createTempDir("bookie", "invalid_journal");
                try {
                    Files.setPosixFilePermissions(
                            journalDirectory.toPath(),
                            Collections.emptySet()
                    );
                } catch (IOException e) {
                    throw new RuntimeException("Failed to create invalid directory", e);
                }
                break;
            }
        }


    @Test
    public void test() {
        try {
            Journal journal = new Journal(journalIndex, journalDirectory, configuration, ledgerDirsManager, statsLogger, byteBufAllocator);

            if (expectedException != null){
                Assert.fail("Expected exception " + expectedException + "but none was thrown");
            }

            Assert.assertNotNull(journal);
            Assert.assertEquals(journalDirectory, journal.getJournalDirectory());
            //After pit
            Assert.assertEquals(journal.getJournalWriteBufferSize(), configuration.getJournalWriteBufferSizeKB() * KB); //Riga 677
            Assert.assertEquals(journal.maxJournalSize, configuration.getMaxJournalSizeMB() * MB); //Riga 675
            
            
            if (isBusyWait) {
                Assert.assertTrue(journal.queue instanceof BlockingMpscQueue);
            } else {
                Assert.assertTrue(journal.queue instanceof BatchedArrayBlockingQueue);
            }

            Assert.assertNotNull(journal.getLastLogMark());

        } catch(Exception e){
            Assert.assertEquals(expectedException, e.getClass());
        }
    }

    @After
    public void tearDown() {
        if (journalDirectory != null && journalDirectory.exists()) {
            deleteRecursively(journalDirectory);
        }
    }

    private void deleteRecursively(File file) {
        try {
            FileUtils.deleteDirectory(file);
        } catch (IOException e) {
            System.err.println("Failed to delete: " + file);
        }
    }

    //Definition Enum

    public enum LedgerDirsManagerStatus {
        NULL,
        VALID,
        INVALID,
    }

    public enum StatsLoggerStatus {
        NULL,
        VALID,
        INVALID
    }

    public enum ConfigurationStatus {
        NULL,
        VALID,
        INVALID,
    }

    public enum JournalDirectoryStatus {
        NULL,
        VALID,
        INVALID,
    }

}
