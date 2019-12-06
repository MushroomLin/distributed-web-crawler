# coding:utf-8
import codecs
import time


class FileSaver(object):
    def __init__(self):
        self.filepath = 'wiki_%s.html' % (time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime()))
        self.output_head(self.filepath)
        self.datas = []

    def store_data(self, data):
        if data is None:
            return
        self.datas.append(data)
        if len(self.datas) > 10:
            self.output_html(self.filepath)

    def output_head(self, path):
        '''
        Write HTML head
        :return:
        '''
        fout=codecs.open(path,'w',encoding='utf-8')
        fout.write("<html>")
        fout.write(r'''<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />''')
        fout.write("<body>")
        fout.close()

    def output_html(self, path):
        '''
        Writing data into H+TML file
        :param path: File path
        :return:
        '''
        fout=codecs.open(path,'a',encoding='utf-8')
        for data in self.datas:
            fout.write("<p>%s</p>"%data['url'])
            fout.write("<p>%s</p>"%data['title'])
            fout.write("<p>%s</p>"%data['summary'])
        self.datas=[]
        fout.close()


    def ouput_end(self, path):
        '''
        Write HTML end label
        :param path: File path
        :return:
        '''
        fout = codecs.open(path, 'a', encoding='utf-8')
        fout.write("</body>")
        fout.write("</html>")
        fout.close()