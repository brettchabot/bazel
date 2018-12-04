// Copyright 2018 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.devtools.build.lib.remote;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.ActionInputPrefetcher;
import com.google.devtools.build.lib.actions.Artifact;
import com.google.devtools.build.lib.actions.CommandLines.ParamFileActionInput;
import com.google.devtools.build.lib.actions.FileArtifactValue;
import com.google.devtools.build.lib.actions.MetadataProvider;
import com.google.devtools.build.lib.actions.cache.VirtualActionInput;
import com.google.devtools.build.lib.profiler.Profiler;
import com.google.devtools.build.lib.profiler.SilentCloseable;
import com.google.devtools.build.lib.remote.util.DigestUtil;
import com.google.devtools.build.lib.remote.util.Utils;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import io.grpc.Context;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.jcip.annotations.GuardedBy;

/**
 * Stages output files that are stored remotely to the local filesystem.
 *
 * <p>This is necessary for remote caching/execution when --experimental_remote_fetch_outputs=none
 * is specified.
 */
public class RemoteActionInputFetcher implements ActionInputPrefetcher {

  private final Object lock = new Object();

  @GuardedBy("lock")
  private final Map<Path, ListenableFuture<Path>> downloadsInProgress = new HashMap<>();

  private final AbstractRemoteActionCache remoteCache;
  private final Path execRoot;
  private final Context ctx;

  public RemoteActionInputFetcher(AbstractRemoteActionCache remoteCache, Path execRoot,
      Context ctx) {
    this.remoteCache = Preconditions.checkNotNull(remoteCache);
    this.execRoot = Preconditions.checkNotNull(execRoot);
    this.ctx = Preconditions.checkNotNull(ctx);
  }

  @Override
  public void prefetchFiles(
      Iterable<? extends ActionInput> inputs, MetadataProvider metadataProvider)
      throws IOException, InterruptedException {
    try (SilentCloseable c = Profiler.instance().profile("Remote.prefetchInputs")) {
      List<ListenableFuture<Path>> downloads = new ArrayList<>();
      List<Path> downloadsPaths = new ArrayList<>();
      for (ActionInput input : inputs) {
        if (input instanceof VirtualActionInput) {
          VirtualActionInput paramFileActionInput = (VirtualActionInput) input;
          Path outputPath = execRoot.getRelative(paramFileActionInput.getExecPath());
          outputPath.getParentDirectory().createDirectoryAndParents();
          try (OutputStream out = outputPath.getOutputStream()) {
            paramFileActionInput.writeTo(out);
          }
        } else {
          FileArtifactValue metadata = metadataProvider.getMetadata(input);
          if (metadata == null || !metadata.isRemote()) {
            continue;
          }

          Path path = execRoot.getRelative(input.getExecPath());
          synchronized (lock) {
            if (path.exists()) {
              continue;
            }

            ListenableFuture<Path> download = downloadsInProgress.get(path);
            if (download == null) {
              Context prevCtx = ctx.attach();
              try {
                download =
                    Futures.transform(
                        remoteCache.downloadFile(
                            path, DigestUtil.buildDigest(metadata.getDigest(), metadata.getSize())),
                        (none) -> path,
                        MoreExecutors.directExecutor());
                downloadsInProgress.put(path, download);
              } finally {
                ctx.detach(prevCtx);
              }
            }
            downloads.add(download);
            downloadsPaths.add(path);
          }
        }
      }

      IOException ioException = null;
      InterruptedException interruptedException = null;
      try {
        for (int i = 0; i < downloads.size(); i++) {
          try {
            Path path = Utils.getFromFuture(downloads.get(i));
            path.setExecutable(true);
            System.err.println(String.format("Fetched %s, %d KiB", path.toString(), path.getFileSize()/1000));
          } catch (IOException e) {
            ioException = ioException == null ? e : ioException;
          } catch (InterruptedException e) {
            interruptedException = interruptedException == null ? e : interruptedException;
          }
        }
      } finally {
        synchronized (lock) {
          for (Path path : downloadsPaths) {
            downloadsInProgress.remove(path);
          }
        }
      }

      if (interruptedException != null) {
        throw interruptedException;
      }
      if (ioException != null) {
        throw ioException;
      }
    }
  }
}
