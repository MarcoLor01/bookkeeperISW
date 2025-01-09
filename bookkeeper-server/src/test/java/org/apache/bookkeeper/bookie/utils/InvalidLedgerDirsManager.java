package org.apache.bookkeeper.bookie.utils;

import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.DiskChecker;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class InvalidLedgerDirsManager extends LedgerDirsManager {

    public InvalidLedgerDirsManager(ServerConfiguration conf, File[] dirs, DiskChecker diskChecker) throws IOException {
        super(conf, dirs, diskChecker);
    }

    @Override
    public List<File> getAllLedgerDirs() {
        throw new IllegalStateException("Ledger directories are in an invalid state");
    }
}
