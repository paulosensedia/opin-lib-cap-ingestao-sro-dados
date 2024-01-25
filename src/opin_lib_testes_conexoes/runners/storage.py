from pathlib import Path

class TestStorage:
    def __init__(self, context):
        self.context = context
    
    @staticmethod
    def path_exists(path):
        if Path(path).exists():
            print(f"Caminho existe: {path}")
            return True
        print(f"Caminho não existe: {path}")
        return False

    def bronze_exists(self):
        return TestStorage.path_exists(self.context.STORAGE_MOUNT_BRONZE)
        
    def silver_exists(self):
        return TestStorage.path_exists(self.context.STORAGE_MOUNT_SILVER)
    
    def gold_exists(self):
        return TestStorage.path_exists(self.context.STORAGE_MOUNT_GOLD)
    
    def transient_exists(self):
        return TestStorage.path_exists(self.context.STORAGE_MOUNT_TRANSIENT)
    

def test_storage(context):
    tst = TestStorage(context)
    tst.bronze_exists()
    tst.silver_exists()
    tst.gold_exists()
    tst.transient_exists()
    return True
    
    
    
