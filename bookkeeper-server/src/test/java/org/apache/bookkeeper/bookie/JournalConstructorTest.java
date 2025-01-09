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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    public JournalConstructorTest(int journalIndex, JournalDirectoryStatus journalDirectoryStatus, ConfigurationStatus configurationStatus,
                                  LedgerDirsManagerStatus ledgerDirsManagerStatus, StatsLoggerStatus statsLoggerStatus,
                                  AllocatorStatus allocatorStatus, Class<? extends Exception> expectedException){
        this.journalIndex = journalIndex;
        this.journalDirectoryStatus = journalDirectoryStatus;
        this.configurationStatus = configurationStatus;
        this.ledgerDirsManagerStatus = ledgerDirsManagerStatus;
        this.statsLoggerStatus = statsLoggerStatus;
        this.allocatorStatus = allocatorStatus;
        this.expectedException = expectedException;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                // TEST: journalIndex, journalDirectory, conf, ledgerDirsManager, statsLogger, allocator -> Expected
                {0, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, null},
                {-1, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, null},
                {1, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, null},
                {0, JournalDirectoryStatus.NULL, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, null},
                {0, JournalDirectoryStatus.VALID, ConfigurationStatus.NULL, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, NullPointerException.class},
                {1, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.NULL, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, NullPointerException.class},
                {0, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.NULL, AllocatorStatus.DEFAULT, NullPointerException.class},
                {1, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.NULL, null},
                {1, JournalDirectoryStatus.INVALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, null},
                {0, JournalDirectoryStatus.VALID, ConfigurationStatus.INVALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, IllegalStateException.class},
                {0, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.INVALID, StatsLoggerStatus.VALID, AllocatorStatus.DEFAULT, IllegalStateException.class},
                {0, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.INVALID, AllocatorStatus.DEFAULT, NullPointerException.class},
                {0, JournalDirectoryStatus.VALID, ConfigurationStatus.VALID, LedgerDirsManagerStatus.VALID, StatsLoggerStatus.VALID, AllocatorStatus.INVALID, null},

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
                File journalDirectory = IOUtils.createTempDir("bookie", "journal");
                BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDirectory));
                this.journalDirectory = journalDirectory;
                break;
            case INVALID:
                Random random = new Random();
                File dir = new File("/target/tmpDirs/journal" + random.nextInt());
                dir.mkdirs();
                dir.setWritable(false, false); //Non scrivibile
                this.journalDirectory = dir;
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

            if (configuration.isBusyWaitEnabled()) {
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
            if (file.isDirectory()) {
                File[] files = file.listFiles();
                if (files != null) {
                    for (File subFile : files) {
                        deleteRecursively(subFile);
                    }
                }
            }
            file.delete();
        } catch (Exception e) {
            System.err.println("Failed to delete file: " + file.getAbsolutePath());
        }
    }


    //DEFINIZIONE VARIE ENUM

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
