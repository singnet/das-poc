import math
import time

import numpy as np
import pytest

from function.das.helpers import get_logger

logger = get_logger()


class Clock:
  def __init__(self):
    self._ini = time.time_ns()

  def reset(self):
    self._ini = time.time_ns()

  def elapsed_time_ns(self):
    return time.time_ns() - self._ini

  def elapsed_time_ms(self):
    return self.elapsed_time_ns() / 1e6

  def elapsed_time_seconds(self):
    return self.elapsed_time_ns() / 1e9


class AccumulatorClock:
  def __init__(self):
    self.acc = 0
    self.ini = time.time_ns()
    self.paused = True

  def reset(self):
    self.acc = 0
    self.ini = time.time_ns()

  def pause(self):
    assert not self.paused
    self.acc += time.time_ns() - self.ini
    self.paused = True

  def start(self):
    assert self.paused
    self.ini = time.time_ns()
    self.paused = False

  def acc_ns(self):
    if self.paused:
      return self.acc
    else:
      return self.acc + time.time_ns() - self.ini

  def acc_ms(self):
    return self.acc_ns() / 1e6

  def acc_seconds(self):
    return self.acc_ns() / 1e9


class Statistics:
  def __init__(self):
    self.n = 0
    self.sum = 0
    self.sum_squared = 0
    self.min = float('inf')
    self.max = float('-inf')

  def reset(self):
    self.n = 0
    self.sum = 0
    self.sum_squared = 0
    self.min = float('inf')
    self.max = float('-inf')

  def add(self, item):
    self.n += 1
    self.sum += item
    self.sum_squared += item ** 2
    if item < self.min:
      self.min = item
    if item > self.max:
      self.max = item

  def mean(self):
    return self.sum / self.n

  def variance(self):
    return (self.sum_squared / self.n) - self.mean() ** 2

  def std(self):
    return math.sqrt(self.variance())

  def __str__(self):
    if self.n == 0:
      return '-'
    else:
      return 'mean: {:.3f}\tstd: {:.3f}\tn: {:9d}\tmin: {}\tmax: {}'.format(
        self.mean(),
        self.std(),
        self.n,
        self.min,
        self.max,
      )

  def pretty_print(self):
    return str(self)


def test_mean():
  s = Statistics()
  v = [1, 2, 3, 4, 5]
  for x in v:
    s.add(x)
  assert np.mean(v) == pytest.approx(s.mean())
  assert np.std(v) == pytest.approx(s.std())


def run():
  acc = AccumulatorClock()
  acc.start()
  time.sleep(1)
  acc.pause()
  time.sleep(1)
  acc.start()
  acc.pause()
  c = Clock()
  c.reset()
  s = Statistics()
  s.add(1)
  s.add(-2)
  s.add(2)
  logger.info(str(s))
  time.sleep(1)
  logger.info(c.elapsed_time_ms())
  logger.info('acc: {}'.format(acc.acc_seconds()))


if __name__ == '__main__':
  run()
