import argparse
import datetime
import urllib.request
import urllib.parse
import urllib.error
import hashlib
import art
from sqlalchemy import select

import config
import zipfile

from dbcontext import Context
from sqlentities import File


class GdletScrapper:

    def __init__(self, context, echo=False, fake_save=False):
        self.url = "http://data.gdeltproject.org/events/index.html"
        self.context = context
        self.echo = echo
        self.fake_save = fake_save
        self.html = ""
        self.rows: list[str] = []
        self.nb_new_file = 0

    def test(self):
        print(f"Open {self.url}")
        try:
            with urllib.request.urlopen(self.url) as response:
                self.html = str(response.read())
                self.rows = self.html.split("<LI>")
                print("OK")
        except Exception as ex:
            print(self.url)
            print(f"WARNING URL Error: {ex}")
            quit(1)

    def daterange(self, start_date: datetime.date, end_date: datetime.date):
        days = int((end_date - start_date).days)
        for n in range(days):
            yield start_date + datetime.timedelta(n)

    def check_md5(self, path: str, md5: str) -> bool:
        with open(path, "rb") as f:
            content = f.read()
            hash = hashlib.md5(content).hexdigest()
            return hash == md5

    def dezip(self, path: str, file: str):
        print(f"Dezip {file} to {config.download_path}")
        with zipfile.ZipFile(path+file, 'r') as zip:
            zip.extractall(config.download_path)

    def download(self, file: File):
        path = f"{config.download_path}/zip/"
        url = self.url.replace("index.html", file.name)
        print(f"Downloading {url} to {path}")
        if self.fake_save:
            file.download_date = file.dezip_date = datetime.datetime.now()
        else:
            try:
                with urllib.request.urlopen(url) as response:
                    content = response.read()
                    with open(path+file.name, "wb") as f:
                        f.write(content)
                    is_md5 = self.check_md5(path+file.name, file.md5)
                    if is_md5:
                        file.download_date = datetime.datetime.now()
                        self.dezip(path, file.name)
                        file.dezip_date = datetime.datetime.now()
                    else:
                        print(f"WARNING bad md5 {file.md5}")
            except Exception as ex:
                print(f"WARNING download Error: {ex}")
        self.nb_new_file += 1

    def get_file_by_id(self, id: int) -> File | None:
        return self.context.session.get(File, id)

    def get_last_file(self) -> File:
        return self.context.session.execute(
            select(File).where(File.dezip_date is not None).order_by(File.online_date.desc())).scalars().first()

    def scrap(self):
        self.test()
        first_date = datetime.date(2013, 4, 1)
        # start_date = self.get_last_file().date + datetime.timedelta(days=-2)
        # if start_date < first_date:
        #     start_date = first_date
        start_date = first_date
        for d in self.daterange(start_date, datetime.date.today()):
            id = int(d.strftime("%Y%m%d"))
            f = self.get_file_by_id(id)
            if f is None:
                name = f"{id}.export.CSV.zip"
                res = [row for row in self.rows if name in row]
                if len(res) > 0:
                    print(f"{name} is in HTML")
                    f = File()
                    f.id = id
                    f.date = d
                    f.name = name
                    f.online_date = datetime.datetime.now()
                    self.context.session.add(f)
                    row = res[0]
                    f.md5 = row[-35:-3]
                    if f.dezip_date is None:
                        self.download(f)
                    self.context.session.commit()
                else:
                    print(f"{name} not in HTML")

    def not_int_html(self):
        self.test()
        first_date = datetime.date(2013, 4, 1)
        for d in self.daterange(first_date, datetime.date.today()):
            id = int(d.strftime("%Y%m%d"))
            name = f"{id}.export.CSV.zip"
            res = [row for row in self.rows if name in row]
            if len(res) == 0:
                print(f"{name} not in HTML")

    def scrap_before_2001304(self):
        self.test()
        for y in range(2006, 2014):
            for m in range(1, 13):
                if y == 2013 and m == 4:
                    break
                f = self.get_file_by_id(y * 100 + m)
                if f is None:
                    f = File()
                    f.id = y * 100 + m
                    f.date = datetime.date(y, m, 1)
                    f.online_date = datetime.datetime.now()
                    self.context.session.add(f)
                if f.dezip_date is None:
                    f.name = f"{y}{m:02d}.zip"
                    row = [row for row in self.rows if f.name in row][0]
                    f.md5 = row[-35:-3]
                    self.download(f)
                self.context.session.commit()

    def scrap_before_2006(self):
        self.test()
        for y in range(1979, 2006):
            f = self.get_file_by_id(y)
            if f is None:
                f = File()
                f.id = y
                f.date = datetime.date(y, 12, 31)
                f.online_date = datetime.datetime.now()
                self.context.session.add(f)
            if f.dezip_date is None:
                f.name = f"{y}.zip"
                row = [row for row in self.rows if f.name in row][0]
                f.md5 = row[-35:-3]
                self.download(f)
            self.context.session.commit()


if __name__ == '__main__':
    art.tprint(config.name, "big")
    print("Gdlet Scrapper")
    print("==============")
    print(f"V{config.version}")
    print(config.copyright)
    print()
    parser = argparse.ArgumentParser(description="Gdlet Parser")
    parser.add_argument("-e", "--echo", help="Sql Alchemy echo", action="store_true")
    parser.add_argument("-o", "--old", help="Download from 1979 to 2005", action="store_true")
    parser.add_argument("-p", "--old2", help="Download from 2006 to 2013", action="store_true")
    parser.add_argument("-f", "--fake_save", help="Faking the save and dezip", action="store_true")
    parser.add_argument("-n", "--no_commit", help="No commit", action="store_true")
    args = parser.parse_args()
    context = Context()
    context.create(echo=args.echo)
    p = GdletScrapper(context, args.echo, args.fake_save)
    if args.old:
        p.scrap_before_2006()
    if args.old2:
        p.scrap_before_2001304()
    # p.not_int_html()
    p.scrap()
    print(f"Nb new files: {p.nb_new_file}")

    # -o
    # -f -o
    # -p


