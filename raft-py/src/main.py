from .persistence import PersistenceLayer

if __name__ == "__main__":
    db_path = "raft.db"
    persistence = PersistenceLayer(db_path)
