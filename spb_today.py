#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime as dt

import vk
from tqdm import tqdm

from sqlalchemy import func, create_engine
from sqlalchemy import Column, Integer, Text, DateTime
from sqlalchemy.schema import Index
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Post(Base):
    __tablename__ = 'post'
    __table_args__ = (Index('post_id_idx', 'post_id'), )

    id = Column(Integer, primary_key=True)
    text = Column(Text, nullable=False)
    author_id = Column(Integer, nullable=False)
    post_id = Column(Integer, nullable=False)
    date = Column(DateTime, nullable=False)
    last_offset = Column(Integer, nullable=False)


def init_db(db_url):
    engine = create_engine(db_url)
    Base.metadata.bind = engine
    Base.metadata.create_all()

    return sessionmaker(bind=engine)


def validate_post(post_response):
    return (('from_id' in post_response) and
            (post_response['marked_as_ads'] == 0) and
            (post_response['post_type'] == 'post'))


if __name__ == '__main__':
    db_session = init_db('sqlite:///test.db')()

    vk_session = vk.Session()
    api = vk.API(vk_session)

    if db_session.query(Post).count() == 0:
        last_offset = 0
    else:
        last_offset = db_session.query(func.max(Post.last_offset)).first()[0]

    total_posts = api.wall.get(domain='spb_today', count=0)[0]

    with tqdm(total=total_posts) as progress:
        progress.update(last_offset)

        while last_offset < total_posts:
            offset = total_posts - last_offset - 100
            response = api.wall.get(domain='spb_today', offset=max(offset, 0),
                    count=100)

            posts = [Post(text=x['text'],
                          author_id=x['from_id'],
                          post_id=x['id'],
                          date=dt.datetime.fromtimestamp(x['date']),
                          last_offset=last_offset + (len(response) - 1 - i))
                    for i, x in enumerate(response[1:]) if validate_post(x)]

            db_session.add_all(posts)
            db_session.commit()

            total_posts = response[0]
            last_offset = db_session.query(
                func.max(Post.last_offset)).first()[0]
            progress.update(len(posts))

    print(db_session.query(Post)[db_session.query(Post).count() - 1].text)
    print(db_session.query(Post)[db_session.query(Post).count() - 1].from_id)
    #for post in db_session.query(Post):
    #    print(post.date)
