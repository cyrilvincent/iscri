import argparse
import datetime
import urllib.request
import urllib.parse
import urllib.error
import hashlib
import art
import config
import zipfile
from dbcontext import Context
from sqlentities import File
from sqlalchemy import select


class GdeltScrapper:

    def __init__(self, context, echo=False, fake_download=False, no_commit=False, force_download=False):
        self.url = "http://data.gdeltproject.org/events/index.html"
        self.context = context
        self.echo = echo
        self.fake_download = fake_download
        self.no_commit = no_commit
        self.force_download = force_download
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
        print(self.fake_download)
        if self.fake_download:
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
        first_date = datetime.date(2022, 11, 1)
        # start_date = self.get_last_file().date + datetime.timedelta(days=-2)
        # if start_date < first_date:
        #     start_date = first_date
        start_date = first_date
        for d in self.daterange(start_date, datetime.date.today()):
            id = int(d.strftime("%Y%m%d"))
            f = self.get_file_by_id(id)
            if f is None or self.force_download:
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
                    self.context.session.add(f)
                else:
                    print(f"{name} not in HTML")
                    break
            if f.dezip_date is None or self.force_download:
                self.download(f)
            if not self.no_commit:
                self.context.session.commit()

    def not_in_html(self):
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
                if f is None or self.force_download:
                    f = File()
                    f.id = y * 100 + m
                    f.date = datetime.date(y, m, 1)
                    f.online_date = datetime.datetime.now()
                    self.context.session.add(f)
                if f.dezip_date is None or self.force_download:
                    f.name = f"{y}{m:02d}.zip"
                    row = [row for row in self.rows if f.name in row][0]
                    f.md5 = row[-35:-3]
                    self.download(f)
                if not self.no_commit:
                    self.context.session.commit()

    def scrap_before_2006(self):
        self.test()
        for y in range(1979, 2006):
            f = self.get_file_by_id(y)
            if f is None or self.force_download:
                f = File()
                f.id = y
                f.date = datetime.date(y, 12, 31)
                f.online_date = datetime.datetime.now()
                self.context.session.add(f)
            if f.dezip_date is None or self.force_download:
                f.name = f"{y}.zip"
                row = [row for row in self.rows if f.name in row][0]
                f.md5 = row[-35:-3]
                self.download(f)
            if not self.no_commit:
                self.context.session.commit()


if __name__ == '__main__':
    art.tprint(config.name, "big")
    print("Gdelt Scrapper")
    print("==============")
    print(f"V{config.version}")
    print(config.copyright)
    print()
    parser = argparse.ArgumentParser(description="Gdelt Parser")
    parser.add_argument("-e", "--echo", help="Sql Alchemy echo", action="store_true")
    parser.add_argument("-o", "--old", help="Download from 1979 to 2005", action="store_true")
    parser.add_argument("-p", "--old2", help="Download from 2006 to 2013", action="store_true")
    parser.add_argument("-f", "--fake_download", help="Faking the download and dezip", action="store_true")
    parser.add_argument("-d", "--force_download", help="Force the download and dezip", action="store_true")
    parser.add_argument("-n", "--no_commit", help="No commit", action="store_true")
    args = parser.parse_args()
    context = Context()
    context.create(echo=args.echo)
    p = GdeltScrapper(context, args.echo, args.fake_download, args.no_commit, args.force_download)
    if args.old:
        p.scrap_before_2006()
    if args.old2:
        p.scrap_before_2001304()
    # p.not_in_html()
    p.scrap()
    print(f"Nb new files: {p.nb_new_file}")

    # -o
    # -f -o
    # -p
    # -o -p -n


