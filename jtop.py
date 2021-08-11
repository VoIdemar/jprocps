"""

Simple script to decorate top-like output with Java thread names
so that they can be further scrutinized in jconsole.

"""

import sys
import curses
import os
from select import select
from argparse import ArgumentParser
import time
import re
from subprocess import Popen, PIPE
import itertools


DEFAULT_ENCODING = "utf-8"


def run_proc(args, env=None, non_zero_exit_code_handler=None):
  proc = Popen(args, stdout=PIPE, env=env)

  # using Popen.stdout directly can cause dealocks while calling Popen.wait(), 
  # thus we use Popen.communicate
  stdout, _ = proc.communicate()
  exit_code = proc.wait()
  if exit_code != 0 and non_zero_exit_code_handler is not None:
    non_zero_exit_code_handler(exit_code)
  
  out_lines = stdout.decode(DEFAULT_ENCODING).split("\n")
  return proc, out_lines


class TopRunner:

  def __init__(self):
    # Map of expected text in the columns to the actual dict layout that we
    # desire.
    self.columns = {
      "PID": "tid",
      "USER": "euser",
      "%CPU": "%cpu",
      "%MEM": "%mem",
      "COMMAND": "comm"
    }

  """
  Return a decorated, CPU-sorted list of Java tids.
  """
  def invoke_top(self, limit=None):
    c_locale_env = os.environ.copy()
    c_locale_env["LC_ALL"] = "C"

    def handle_top_err(exit_code):
      raise Exception("top failed with exit code {}".format(exit_code))

    _, top_out_lines = run_proc(
      ["top", "-H", "-b", "-n", "1"],
      env=c_locale_env,
      non_zero_exit_code_handler=handle_top_err
    )

    expected_cols = list(self.columns.keys())
    header_layout, data_start_idx = self._skip_header(top_out_lines, expected_cols)

    output = map(
      lambda line: self._parse_top_line(line, header_layout),
      filter(
        lambda line: line.strip() != "",
        top_out_lines[data_start_idx:]
      )
    )
    
    # Sort and trim.
    output = sorted(
      output,
      key=lambda row: float(row["%cpu"]),
      reverse=True
    )

    java_top = list(filter(lambda row: row["comm"] == "java", output))
    if limit is not None:
      return java_top[:limit]
    else:
      return java_top

  """
  Read the provided input until the top column header line is read.

  Returns the header layout or raises an exception if the header line is not
  encountered.

  """
  def _skip_header(self, lines, expected_headers):
    first_data_line_idx = 0
    for line in lines:
      row = line.split(None)
      row_expected = list(filter(lambda col: col in expected_headers, row))
      if len(row_expected) == len(expected_headers):
        return (row, first_data_line_idx + 1)
      first_data_line_idx += 1

    raise Exception(
      "Could not find top header row, expected: {}".format(" ".join(expected_headers))
    )

  def _parse_top_line(self, line, header_layout):
    untidied_row = dict(zip(header_layout, line.split(None)))

    tidied_row = {}
    for untidy_key, untidy_value in untidied_row.items():
      if untidy_key in self.columns.keys():
        tidied_row[self.columns[untidy_key]] = untidy_value
    tidied_row["pid"] = self._get_pid_from_tid(tidied_row["tid"])

    return tidied_row

  def _get_pid_from_tid(self, tid):
    status_file = "/proc/{}/status".format(int(tid))
    try:
      with open(status_file) as f:
        for line in f:
          row = line.split(None)
          if row[0] == "Tgid:":
            return row[1]
        raise Exception("Failed to determine pid from {}".format(status_file))
    except IOError:
      return None


class PSAndJStackMerger:

  def __init__(self):
    self.top_runner = TopRunner()

  """
  Return a dict of tid to thread name.
  """
  def _get_thread_names(self, pid):
    _, jstack_lines = run_proc(["jstack", pid])

    names = {}
    for line in jstack_lines:
      # jstack calls them nids and uses tid for a Java-specific concept
      # (unrelated to what top calls a pid/tid)
      match = re.search('^\"(.*?)\" .*nid=(0x[a-f0-9]+)', line)
      if match:
        thread_name = match.group(1)
        tid = int(match.group(2), 0)
        names[tid] = thread_name

    return names

  """
  Add thread_name to a list of ps entries (and return those entries).
  """
  def _decorate_ps_with_thread_name(self, ps):
    by_pid_func = lambda row: row["pid"]
    ps_by_pid = sorted(ps, key=by_pid_func)

    for pid, tid_rows in itertools.groupby(ps_by_pid, key=by_pid_func):
      names = self._get_thread_names(pid)
      for tid_row in tid_rows:
        tid = int(tid_row["tid"])
        thread_name = None
        if int(pid) == tid:
          thread_name = "<main>"
        elif tid in names:
          thread_name = names[tid]
        else:
          thread_name = "???"
        tid_row["thread_name"] = thread_name
    return ps_by_pid

  def list_threads(self, limit=None):
    top_output = self.top_runner.invoke_top(limit)
    return self._decorate_ps_with_thread_name(top_output)

class JTop:

  def __init__(self):
    self.merger = PSAndJStackMerger()
    self.done = False
    self.dirty_screen = False

  def print_ps(self, ps, suffix=None, max_width=None):
    base_fmt = "{:9s} {:9s} {:<10s} {:<5s} {:<5s} "
    base_fmt_width = len(base_fmt.format(*itertools.repeat("", 5)))
    if max_width is not None:
      # TODO: Should try to handle very small terminal windows...
      remaining = max_width - base_fmt_width
      if remaining <= 0:
        raise Exception("Very small terminals not supported")
      thread_name_fmt = "{:" + str(remaining) + "s}"
    else:
      thread_name_fmt = "{}"
    fmt = base_fmt + thread_name_fmt
    if suffix is not None:
      fmt = fmt + suffix
    print(fmt.format("PID", "TID", "USER", "%CPU", "%MEM", "THREAD"))
    columns = ("pid", "tid", "euser", "%cpu", "%mem", "thread_name")
    for proc in ps:
      print(fmt.format(*map(lambda key: proc[key], columns)))

  def run_once(self):
    self.print_ps(self.merger.list_threads())

  def _mark_dirty(self):
    self.dirty_screen = True

  def _mark_clean(self):
    self.dirty_screen = False

  def _process_input(self, ch):
    if ch in range(256):
      cch = chr(ch)
      if cch == "q" or cch == "Q":
        self.done = True
        self._mark_dirty()

  def _sleep_and_process_input(self, screen):
    delay = self.options.delay
    while delay > 0:
      start = time.time()
      (read_ready, _, _) = select([sys.stdin], [], [], delay)
      elapsed = time.time() - start
      delay -= elapsed
      if read_ready:
        self._process_input(screen.getch())
        if self.dirty_screen:
          break
      else:
        break

  def run_continuously(self):
    # Disclaimer: I have no idea what I"m doing when it comes to ncurses.
    # Absolute black magic hackery here based on my light read of procps"
    # top implementation.
    def runner(screen):
      curses.use_default_colors()
      first_run = True
      while not self.done:
        (maxy, maxx) = screen.getmaxyx()
        ps = self.merger.list_threads(maxy - 2)
        self.last_ps = ps
        # Weird little hack seems to be needed on the first draw pass else
        # it will not take effect.  Draw twice on first run.
        for _ in range(2 if first_run else 1):
          self.print_ps(ps, "\r", maxx)
          screen.refresh()
        self._mark_clean()
        first_run = False
        self._sleep_and_process_input(screen)
    
    curses.wrapper(runner)

    # Lame hack to put the last output on screen after "q" is hit.
    # curses.wrapper is presumed to be clearing the screen on exit, which
    # we definitely don"t want.
    if self.last_ps is not None:
      self.print_ps(self.last_ps)

  def run(self, args):
    try:
      self._run(args)

      # Work around: http://www.velocityreviews.com/forums/t749747-help-with-a-piping-error.html
      sys.stdout.flush()
    except KeyboardInterrupt:
      sys.exit(130)
    except (IOError, OSError) as ex:
      print(ex)
      sys.exit(141)

  def _run(self, args):
    parser = ArgumentParser(
      description="top for Java threads"
    )
    parser.add_argument(
      "-b", "--batch", action="store_true",
      help="Run in batch mode (see top)"
    )
    parser.add_argument(
      "-d", "--delay", metavar="N", type=float,
      default=2,
      help="Sleep <N> seconds between runs"
    )
    self.options, _ = parser.parse_known_args(args)
    if self.options.batch:
      self.run_once()
    else:
      self.run_continuously()

jtop = JTop()
jtop.run(sys.argv)
