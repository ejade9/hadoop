/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.*;

public class Ezcopy {

    public static final Log LOG = LogFactory.getLog(Ezcopy.class);
    protected final Configuration conf;

    private String clientName;
    public static int THREAD_POOL_SIZE = 5;
    private final ExecutorService executor;

    private DistributedFileSystem srcFileSystem = null;
    private DistributedFileSystem dstFileSystem = null;

    private boolean skipUnderConstructionFile = false;

    public enum CopyResult {SUCCESS, SKIP, FAIL}

    public Ezcopy(Configuration conf, int threadPoolSize,
                  boolean skipUnderConstructionFile, DistributedFileSystem srcFileSystem,
                  DistributedFileSystem dstFileSystem) throws Exception {
        this.conf = conf;
        this.executor = Executors.newFixedThreadPool(threadPoolSize);
        this.skipUnderConstructionFile = skipUnderConstructionFile;

        this.srcFileSystem = srcFileSystem;
        this.dstFileSystem = dstFileSystem;
        this.clientName = dstFileSystem.getClient().getClientName();
    }


    private class ezcopyCopy implements Callable<CopyResult> {
        private final String source;
        private final String destination;

        private final ClientProtocol srcNamenode;
        private final ClientProtocol dstNamenode;
        private final DistributedFileSystem dstFs;
        private final DistributedFileSystem srcFs;
        private final boolean isIncluster;

        public ezcopyCopy(String src, String destination,
                          DistributedFileSystem srcFs, DistributedFileSystem dstFs) throws Exception {
            this.srcNamenode = srcFs.getClient().getNamenode();
            this.dstNamenode = dstFs.getClient().getNamenode();

            if (dstFs.getUri().toString().equals(srcFs.getUri().toString())) {
                isIncluster = true;
            }
            else
                isIncluster = false;
            // if incluster, remove the hdfs:// prefix and only use path component.
            if (isIncluster) {
                this.source = new Path(src).makeQualified(srcFs.getUri(), srcFs.getWorkingDirectory()).toUri().getPath();
                this.destination = new Path(destination).makeQualified(dstFs.getUri(), dstFs.getWorkingDirectory()).toUri()
                        .getPath();
            }
            else {
                this.source = new Path(src).makeQualified(srcFs.getUri(), srcFs.getWorkingDirectory()).toUri().getPath();
                this.destination = destination;
            }
            this.srcFs = srcFs;
            this.dstFs = dstFs;
        }

        public CopyResult call() throws Exception {
            srcNamenode.ezcopy(source, destination, dstFs.getClient().getClientName(), isIncluster);
            return CopyResult.SUCCESS;
        }

    }

    public void shutdown() throws IOException {
        executor.shutdownNow();
    }


    public void copy(List<ezcopyRequest> requests) throws Exception {
        List<Future<CopyResult>> results = new ArrayList<>();

        for (ezcopyRequest r : requests) {
            Callable<CopyResult> ezcopyCopy = new ezcopyCopy(r.getSrc(),
                    r.getDestination(), r.srcFs, r.dstFs);
            Future<CopyResult> f = executor.submit(ezcopyCopy);
            results.add(f);
        }

        for (Future<CopyResult> f : results) {
            f.get();
        }
    }


    public static class ezcopyRequest {

        private final String src;
        private final String dst;
        private final DistributedFileSystem srcFs;
        private final DistributedFileSystem dstFs;

        public ezcopyRequest(String src, String dst,
                             DistributedFileSystem srcFs, DistributedFileSystem dstFs) {
            this.src = src;
            this.dst = dst;
            this.srcFs = srcFs;
            this.dstFs = dstFs;
        }

        /**
         * @return the src
         */
        public String getSrc() {
            return src;
        }

        /**
         * @return the destination
         */
        public String getDestination() {
            return dst;
        }
    }

    /**
     * Wrapper class that holds the source and destination path for a file to be
     * copied. This is to help in easy computation of source and destination files
     * while copying directories.
     */

    private static class CopyPath {
        private final Path srcPath;
        private final Path dstPath;

        /**
         * @param srcPath source path from where the file should be copied from.
         * @param dstPath destination path where the file should be copied to
         */
        public CopyPath(Path srcPath, Path dstPath) {
            this.srcPath = srcPath;
            this.dstPath = dstPath;
        }

        /**
         * @return the srcPath
         */
        public Path getSrcPath() {
            return srcPath;
        }

        /**
         * @return the dstPath
         */
        public Path getDstPath() {
            return dstPath;
        }

    }

    private static Options options = new Options();
    private static Configuration defaultConf = new Configuration();


    private static void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Usage : ezcopy [options] <srcs...> <dst>", options);
    }

    private static CommandLine parseCommandline(String args[]) throws ParseException {
        options.addOption("t", "threads", true, "The number of concurrent theads" + " to use");
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);
        return cmd;
    }

    /**
     * Recursively lists out all the files under a given path.
     *
     * @param root   the path under which we want to list out files
     * @param fs     the filesystem
     * @param result the list which holds all the files.
     * @throws IOException
     */
    private static void getDirectoryListing(FileStatus root, FileSystem fs,
                                            List<CopyPath> result, Path dstPath) throws IOException {
        if (!root.isDirectory()) {
            result.add(new CopyPath(root.getPath(), dstPath));
            return;
        }

        for (FileStatus child : fs.listStatus(root.getPath())) {
            getDirectoryListing(child, fs, result, new Path(dstPath, child.getPath().getName()));
        }
    }

    /**
     * Get the listing of all files under the given directories.
     *
     * @param fs    the filesystem
     * @param paths the paths whose directory listing is to be retrieved
     * @return the directory expansion for all paths provided
     * @throws IOException
     */
    private static List<CopyPath> expandDirectories(FileSystem fs,
                                                    List<Path> paths, Path dstPath)
            throws IOException {
        List<CopyPath> newList = new ArrayList<>();
        FileSystem dstFs = dstPath.getFileSystem(defaultConf);

        boolean isDstFile = false;
        try {
            FileStatus dstPathStatus = dstFs.getFileStatus(dstPath);
            if (!dstPathStatus.isDirectory()) {
                isDstFile = true;
            }
        } catch (FileNotFoundException e) {
            isDstFile = true;
        }

        for (Path path : paths) {
            FileStatus pathStatus = fs.getFileStatus(path);
            if (!pathStatus.isDirectory()) {
                // This is the case where the destination is a file, in this case, we
                // allow only a single source file. This check has been done below in
                // Ezcopy#parseFiles(List, String[])
                if (isDstFile) {
                    newList.add(new CopyPath(path, dstPath));
                } else {
                    newList.add(new CopyPath(path, new Path(dstPath, path.getName())));
                }
            } else {
                // If we are copying /a/b/c into /x/y/z and 'z' does not exist, we
                // create the structure /x/y/z/f*, where f* represents all files and
                // directories in c/
                Path rootPath = dstPath;
                // This ensures if we copy a directory like /a/b/c to a directory
                // /x/y/z/, we will create the directory structure /x/y/z/c, if 'z'
                // exists.
                if (dstFs.exists(dstPath)) {
                    rootPath = new Path(dstPath, pathStatus.getPath().getName());
                }
                getDirectoryListing(pathStatus, fs, newList, rootPath);
            }
        }
        return newList;
    }

    /**
     * Expand a single file, if its a file pattern list out all files matching the
     * pattern, if its a directory return all files under the directory.
     *
     * @param src     the file to be expanded
     * @param dstPath the destination
     * @return the expanded file list for this file/filepattern
     * @throws IOException
     */
    private static List<CopyPath> expandSingle(Path src, Path dstPath)
            throws IOException {
        List<Path> expandedPaths = new ArrayList<Path>();
        FileSystem fs = src.getFileSystem(defaultConf);
        FileStatus[] stats = fs.globStatus(src);
        if (stats == null || stats.length == 0) {
            throw new IOException("Path : " + src + " is invalid");
        }
        for (FileStatus stat : stats) {
            expandedPaths.add(stat.getPath());
        }
        List<CopyPath> expandedDirs = expandDirectories(fs, expandedPaths, dstPath);
        return expandedDirs;
    }

    /**
     * Expands all sources, if they are file pattern expand to list out all files
     * matching the pattern, if they are a directory, expand to list out all files
     * under the directory.
     *
     * @param srcs    the files to be expanded
     * @param dstPath the destination
     * @return the fully expanded list of all files for all file/filepatterns
     * provided.
     * @throws IOException
     */
    private static List<CopyPath> expandSrcs(List<Path> srcs, Path dstPath)
            throws IOException {
        List<CopyPath> expandedSrcs = new ArrayList<CopyPath>();
        for (Path src : srcs) {
            expandedSrcs.addAll(expandSingle(src, dstPath));
        }
        return expandedSrcs;
    }

    private static String parseFiles(List<CopyPath> expandedSrcs, String args[])
            throws IOException {
        if (args.length < 2) {
            printUsage();
            System.exit(1);
        }

        List<Path> srcs = new ArrayList<>();
        for (int i = 0; i < args.length - 1; ++i) {
            srcs.add(new Path(args[i]));
        }

        String dst = args[args.length - 1];
        Path dstPath = new Path(dst);
        expandedSrcs.clear();
        expandedSrcs.addAll(expandSrcs(srcs, dstPath));

        FileSystem dstFs = dstPath.getFileSystem(defaultConf);

        // If we have multiple source files, the destination has to be a directory.
        if (dstFs.exists(dstPath) && !dstFs.getFileStatus(dstPath).isDirectory()
                && expandedSrcs.size() > 1) {
            printUsage();
            throw new IllegalArgumentException("Path : " + dstPath + " is not a directory");
        }
        // If the expected destination is a directory and it does not exist throw an error.
        if (!dstFs.exists(dstPath) && srcs.size() > 1) {
            printUsage();
            throw new IllegalArgumentException("Path : " + dstPath + " does not exist");
        }
        return dst;
    }


    public static void runTool(String args[]) throws Exception {
        // parse the args into srcs and dst
        CommandLine cmd = parseCommandline(args);
        args = cmd.getArgs();
        int threadPoolSize = (cmd.hasOption('t')) ? Integer.parseInt(cmd
                .getOptionValue('t')) : THREAD_POOL_SIZE;

        List<CopyPath> srcs = new ArrayList<>();
        String dst = parseFiles(srcs, args);

        Path dstPath = new Path(dst);
        DistributedFileSystem dstFileSys = (DistributedFileSystem) dstPath
                .getFileSystem(defaultConf);

        DistributedFileSystem srcFileSys = (DistributedFileSystem) (srcs.get(0)
                .getSrcPath().getFileSystem(defaultConf));

        List<ezcopyRequest> requests = new ArrayList<>();
        Ezcopy ezcopy = new Ezcopy(new Configuration(), threadPoolSize, false, srcFileSys, dstFileSys);
        try {
            for (CopyPath copyPath : srcs) {
                Path srcPath = copyPath.getSrcPath();
                String src = srcPath.toString();
                try {
                    // Perform some error checking and path manipulation.
                    if (!srcFileSys.exists(srcPath)) {
                        throw new IOException("File : " + src + " does not exists on " + srcFileSys);
                    }
                    String destination = copyPath.getDstPath().toString();
                    LOG.debug("Copying : " + src + " to " + destination);
                    requests.add(new ezcopyRequest(src, destination, srcFileSys, dstFileSys));
                } catch (Exception e) {
                    LOG.warn("Ezcopy failed for file : " + src, e);
                }
            }
            ezcopy.copy(requests);
            LOG.debug("Finished copying");
        } finally {
        }
    }

    public static void main(String args[]) throws Exception {
        runTool(args);
        System.exit(0);
    }
}
