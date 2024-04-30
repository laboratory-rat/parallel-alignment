from pydantic import BaseModel


class SequenceData(BaseModel):
    id: str
    name: str
    sequence: str
    description: str

    @staticmethod
    def from_fasta(fasta) -> 'SequenceData':
        return SequenceData(
            id=fasta.id,
            name=fasta.name,
            sequence=str(fasta.seq),
            description=fasta.description
        )

    def __str__(self):
        return f'> {self.id} {self.name} {self.description} \n {self.sequence}'
