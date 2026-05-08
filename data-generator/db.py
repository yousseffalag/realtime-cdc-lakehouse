from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config.settings import settings

DATABASE_URL = (
    f"postgresql+psycopg2://{settings.DB_CONFIG['user']}:"
    f"{settings.DB_CONFIG['password']}@"
    f"{settings.DB_CONFIG['host']}:"
    f"{settings.DB_CONFIG['port']}/"
    f"{settings.DB_CONFIG['database']}"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)


class DBSession:
    def __enter__(self):
        self.session = SessionLocal()
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                self.session.rollback()
            else:
                self.session.commit()
        finally:
            self.session.close()
