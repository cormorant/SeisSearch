#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division
#
#  SearchParts.py
#  
#  Copyright 2016 petr <petr@SEISMOGRAMMA>
#  
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#  
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#  
"""
ТЗ:
задан исходный каталог (текстовый файл) с землетрясениями
(на самом деле нужны только 2 столбца: 1 - дата, 2 - время).
По заданному файлу в указанной папке происходит:
 - предварительная индексация файлов, запись метаданных в БД
 - поиск в БД требуемых файлов для каждого землетрясения
 - запись в выходную папку файлов с вырезками; файлы заданной длины

TODO: проблема с 1 секундой: данные из буфера не совпадает с вырезкой на 1 сек.

Example run:
python searchparts.py -f catalog.txt D:\Work\seis\data\baikal\uud\2015
"""
__version__="0.0.1"
COMPANY_NAME = 'GIN SB RAS'
APP_NAME = "SearchParts"

import os
import sys
import struct
import datetime
import math
#TODO: нужен только dstask или еще загрузка файлов
import numpy as np
#
import sqlite3
import argparse


#=== SQL queries
SQL_CODE = """\
CREATE TABLE IF NOT EXISTS "data" (
    "id" integer NOT NULL PRIMARY KEY,
    "filename" varchar(128),
    "kan" integer unsigned,
    "date" date NOT NULL,
    "razr" integer unsigned,
    "station" varchar(16),
    "sampling" real,
    "first_sec" real NOT NULL,
    "length" integer
);"""

InsertInto = """\
INSERT INTO "data" (
    filename, kan, date, razr, station, sampling, first_sec, length
) VALUES (?, ?, ?, ?, ?, ?, ?, ?);"""

SEARCH_QUERY = """\
SELECT filename, kan, razr, first_sec, (first_sec+length*sampling) as last_sec,
    station, sampling, length, date
FROM data
WHERE date = ? AND
    (? BETWEEN first_sec AND last_sec
    OR first_sec BETWEEN ? AND ?)
ORDER BY date, first_sec;"""

#=== Baikal format definitions
#=== Краткое описание типов данных
# имена
MainHeaderNames = ('nkan', 'test', 'vers', 'day', 'month', 'year', 'satellit',
    'valid', 'pri_synhr', 'razr', 'reserv_short1', 'reserv_short2',
    'reserv_short3', 'reserv_short4', 'reserv_short5', 'reserv_short6',
    'station', 'dt', 'to', 'deltas', 'latitude', 'longitude')
# заголовок файла
MainHeaderTypeStruct = '16h16s5d'
MainHeaderTypeStructSize = struct.calcsize(MainHeaderTypeStruct)
#===

def parse_file_to_dict(filename, counter):
    """ Читаются файлы, отуда вытаскивается информация """
    # read header
    with open(filename, "rb") as _f:
        try:
            nkan = struct.unpack("h", _f.read(2))[0]
        except struct.error:
            print("\nStruct.error in file %s" % filename)
            return counter
        # проверка на количество каналов
        if not nkan in range(1, 7):
            print("\nSkipping file %s" % filename)
            return counter
        # подготовим заголовок
        header = {"filename": filename, "nkan": nkan}
        # считаем данные
        _f.seek(0)
        data = struct.unpack(MainHeaderTypeStruct,
            _f.read(MainHeaderTypeStructSize))
        # обновим заголовок
        header.update( dict(zip(MainHeaderNames, data)) )
        # поправим станцию
        header["station"] = header["station"][:3].strip()
        #= неправильный год кое-где
        # м.б. значения например 99, значит год 1999
        if 90 <= header["year"] <= 99:
            header["year"] += 1900
        #? what to do with 2000 year (00)?
        elif header["year"] == 0:
            header["year"] = 2000
        elif 89 <= header["year"] <= 99:
            header["year"] += 1900
        elif header["year"] < 1900:
            header["year"] += 2000
        else:
            pass
        # получим дату
        try:
            # получить дату/время из заголовка
            date = datetime.date(header["year"], header["month"], header["day"])
        except ValueError, msg:
            print("\nDatetime parsing error: %s" % msg)
            return counter
        else:
            # проверка что наша дата есть в списке дат для поиска
            if date in date_list:
                header['date'] = date
            else:
                # nice output
                s = "%s %s" % (counter, filename)
                sys.stdout.write("\r" + s)
                sys.stdout.flush()
                return counter
        # читаем массив с данными из файла непрерывки
        dtype = np.int16 if header['razr'] == 16 else np.int32
        # где начинать считывать данные
        offset = 120 + nkan * 72
        _f.seek(offset)
        # load&read
        a = np.fromstring(_f.read(), dtype=dtype)
        # обрезать массив с конца пока он не делится на 3
        while len(a) % nkan != 0:
            a = a[:-1]
        # демультиплексируем
        a = a.reshape( (int(a.size / nkan), nkan) ).T
        # length of file
        header["length"] = a[0].size
    # ++counter
    counter += 1
    # nice output
    s = "%s %s" % (counter, filename)
    sys.stdout.write("\r" + s)
    sys.stdout.flush()
    # save new value
    values = [header[key] for key in ("filename", "nkan", "date", "razr",
        "station", "dt", "to", 'length')]
    # execute query
    cursor.execute(InsertInto, values)
    #conn.commit()
    return counter


def recursive_search(path, counter):
    """ Рекурсивная обработка файлов """
    if os.path.isfile(path):
        counter = parse_file_to_dict(path, counter)
    elif os.path.isdir(path):
        for file in (os.path.join(path, file) for file in os.listdir(path)):
            counter = recursive_search(file, counter)
    else:
        print("Problem with path: %s" % (path))
    return counter


def save_and_index_files(path):
    """ предварительная индексация файлов, запись метаданных в БД """
    cursor.execute(SQL_CODE)
    # работаем с переданной папкой
    print("\nIndexing %s" % path)
    # получить список значений, пройдя по всем папкам в указанном пути
    counter = 0
    # начинать поиск
    try:
        counter = recursive_search(path, counter)
        # searching done
        print("\nPath %s, found %s file(s)." % (path, counter))
    finally:
        conn.commit()
    print
    return 0


def get_time(t0):
    """ Возвращаем вычисленное время из числа секунд. 'to' -- это Т0 (T нулевое) """
    # t0 - должно быть число
    hours, remainder = divmod(t0, 3600)
    minutes, seconds = divmod(remainder, 60)
    microseconds, seconds = math.modf(seconds)
    # multiple ms by 1mln seconds
    microseconds *= 1e6
    return datetime.time(*map(int, (hours, minutes, seconds, microseconds)))


def read_data(filename, nkan, razr, read_header=False, start=None, end=None):
    """ считывать главный заголовок и область данных """
    with open(filename, "rb") as _f:
        # заголовок с каналами (если нужно)
        if read_header:
            #header = _f.read(120 + nkan * 72)
            header = _f.read(120 + 3 * 72) # dirty hack: read only 3 channel headers
        else:
            header = None
            _f.seek(120 + nkan * 72)
        # массив с данными из файла непрерывки
        dtype = np.int16 if razr==16 else np.int32
        a = np.fromstring(_f.read(), dtype=dtype)
    # демультиплексировать
    a = a.reshape((int(a.shape[0] / nkan), nkan)).T
    # возвращать указанные индексы массивов
    return header, a[:3, start:end]


def make_filename(Datetime, station, path, suffix=None):
    """ путь для файла из даты-времени и папки вида: ММ_ДД/_ММ_ДД_ЧЧ_ММ """
    # правильный формат имени файлов и папок в seisobr:
    # 2014 / 14_01 / _01_01_11_27 / 201301011127hrm.5h
    # где:
    # YYYY / YY_MM / MM_DD_HH_MM / YYYYMMDDHHMM.0h
    # make dirs
    newPath = os.path.join(path, "{:%Y/%y_%m/%m_%d_%H_%M}".format(Datetime) )
    #os.path.join(path, date.strftime("%Y"), date.strftime("%m_%d"))
    #"_%s_%s" % (date.strftime("%m_%d"), time.strftime("%H_%M"))
    if not os.path.exists(newPath): os.makedirs(newPath)
    # имя для нового файла:
    # YYYYMMDDHHMM.0h (aka 201301011127hrm.0h)
    newfname = "{0:%Y%m%d_%H%M}{1}.0{2}".format(Datetime, station.lower(),
        station[0].upper() if suffix is None else "i") # если иркут добавить i
    # полный путь и имя файла для записи
    fullpath = os.path.join(newPath, newfname)
    return fullpath


def write_file(result, T0, T1, out_dir, Datetime):
    """ функция записывает файл из нескольких переданных, начиная с T0 до T1 """
    # считать 1й файл
    filename, nkan, razr, first_sec, last_sec, station, sampling, length, date = result[0]
    print("Writing file from %s" % filename)
    # подготовим выходной файл #TODO: add YEAR to resulting path
    fullpath = make_filename(Datetime, station, out_dir)
    # создадим файл
    _f = open(fullpath, "wb")
    #=== 1-й файл
    # нужно вычислить начальный индекс массива откуда начинаются нужные данные (округлить)
    start = int( round( (T0 - first_sec) / sampling ) )
    if start < 0:
        # если начало файла после начала выборки, то брать весь файл
        raise BaseException("What to do if start of file is after start time?")
        #!!!!! T0 = ????????????????
    # считать заголовок и данные
    header, data = read_data(filename, nkan, razr, read_header=True, start=start)
    #=== записываем в файл
    # write initial header and data
    _f.write(header)
    _f.write( data.T.flatten().tostring() )
    #=== промежуточные файлы (и до конца)
    for filename, nkan, razr, _, _, next_station, srg, _len, _ in result[1:]:
        # проверка что это та же самя станция
        if not station == next_station:
            print("Cannot concatenate data from files with different station!")
            break
        # считать файл полностью и дописать
        header, data = read_data(filename, nkan, razr, read_header=False)
        _f.write( data.T.flatten().tostring() )
    #TODO=== завершающий файл, вычислить последнюю секунду ктр нужна???
    #ошибка: может быть 6 каналов, а мы считали только три: исправить заголовок!
    #if nkan != 3:
    _f.seek(0)
    _f.write( struct.pack('h', 3) )
    # перезаписать 1-ю секунду правильную
    _f.seek(56)
    _f.write( struct.pack('d', T0) )
    # закрыть получившийся файл
    _f.close()


def search_eqs_in_db(dates, MinutesBefore, MinutesAfter):
    """ поиск в БД требуемых файлов для каждого землетрясения """
    for dt in dates:
        # вычислить число секунд
        dt0 = datetime.datetime.combine(dt, datetime.time(0))
        time_delta = dt - dt0
        #seconds = (dt - dt0).total_seconds()
        seconds = (time_delta.microseconds/1e6 + time_delta.seconds +
            time_delta.days * 24 * 3600)
        # calc first and last second needed
        first_sec = seconds - 60 * MinutesBefore# 1 minute before
        last_sec = seconds + 60 * MinutesAfter# 5 minutes after
        # params for query: date, first_sec, first_sec, last_sec
        params = (dt.date(), first_sec, first_sec, last_sec)
        # execute query
        cursor.execute( SEARCH_QUERY, params)
        result = cursor.fetchall()
        # write files from start to end to get one resulting file
        if result:
            if args.verbose:
                print("+"*33)
                print result, first_sec, last_sec
                print("+"*33)
            write_file(result, first_sec, last_sec, args.out, dt)
        else:
            print("No result for event time %s" % dt)


def read_catalog_file(_f):
    """ считать из каталога информацию о дате и времени (первые два столбца) """
    # получить строки из файла
    lines = [line.strip() for line in _f.readlines() if line]
    dates = []
        for num_line, line in enumerate(lines):
            # make datetime from first two columns
            try:
                # use UTCDateTime obj
                dt = UTCDateTime("T".join(line.split()[:2]), precision=3)
            except ValueError as e:
                if num_line == 0: print("Skipping first line. May be a header?")
                else: print("Error parsing datetime on line %d: %s" % (num_line, e))
            else:
                dates += [dt]
    # вернуть список дат/времени для поиска
    return dates


def main(args, dates):
    #===
    # предварительная индексация файлов, запись метаданных в БД
    # process every dir in given args
    for path in args.path:
        # check path
        if not os.path.exists(path) or not os.path.isdir(path):
            print("Path %s not found or isn't a directory! Check it." % path)
            continue
        # run indexing function
        try:
            save_and_index_files(path)
        except KeyboardInterrupt:
            print("\nInterrupted by user...")
    #===
    # поиск в БД требуемых файлов для каждого землетрясения
    search_eqs_in_db(dates, args.before, args.after)
    #===
    return 0


if __name__ == '__main__':
    #=== argparse part
    # парсер опций и аргументов
    parser = argparse.ArgumentParser(description=APP_NAME)
    # version
    parser.add_argument('-V', '--version', action='version',
        version='%(prog)s , v. ' +__version__)
    # verbose mode
    parser.add_argument("-v", "--verbose", action='store_true', help='verbose mode')
    # где искать данные
    parser.add_argument("path", nargs="+", help="directory to search and index")
    # database filename to use
    parser.add_argument("-d", "--db", default=":memory:",
        help="database filename to use")
    # file with catalog
    parser.add_argument("-f", "--file", type=argparse.FileType("r"),
        required=True, help="input file with catalog of earthquakes")
    # scan all files or not
    parser.add_argument("-a", "--all", action="store_true",
        help="scan all files instead of matching given days from catalog")
    # сколько минут брать до и после нужного времени из каталога
    # before
    parser.add_argument("--before", default='1', type=int,
        help="minutes before")
    # after
    parser.add_argument("--after", default='4', type=int,
        help="minutes after")
    # option where to put final files
    parser.add_argument("-o", "--out", default='out',
        help="directory where to put output files (default is 'out')")
    #= parse arguments
    args = parser.parse_args()
    if args.verbose: print args
    #=== endof argparse part
    #= prepare information from catalog file
    # считать файл с каталогом, вытащить из него первые два столбца
    try:
        dates = read_catalog_file(args.file)
    except BaseException as e:
        print("Error parsing dates from catalog file! Check it.")
        sys.exit(0)
    else:
        if not dates:
            print("Found 0 date/time string to search. Nothing to do! Exiting.")
            sys.exit(0)
        else:
            print("Read %d lines from file %s" % (len(dates), args.file.name))
            # make also list of date
            date_list = [d.datetime.date() for d in dates]
    #=
    # prepare database
    database_name = args.db
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()
    #conn.text_factory = str
    #=== Main func
    try:
        main(args, dates)
    finally:
        conn.close()
    #=== endof Main func
