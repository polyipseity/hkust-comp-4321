from asyncio import gather
from enum import StrEnum
from itertools import chain
from typing import NamedTuple, Self, Type, TypeVar, cast
from tortoise import Model
from tortoise.fields import (
    BigIntField,
    CASCADE,
    CharField,
    DatetimeField,
    FloatField,
    ForeignKeyField,
    ForeignKeyNullableRelation,
    ForeignKeyRelation,
    ManyToManyField,
    ManyToManyRelation,
    OneToOneField,
    OneToOneRelation,
    OneToOneNullableRelation,
    RESTRICT,
    TextField,
)
from tortoise.functions import Max
from tortoise.transactions import atomic
from tortoise.validators import (
    CommaSeparatedIntegerListValidator,
    MaxValueValidator,
    MinValueValidator,
)

from .. import NAME
from ..index import IndexedPage

_TExtendsModel = TypeVar("_TExtendsModel", bound=Model)

APP_NAME = NAME
"""
App name of the models.
"""


def default_config(connection: str):
    """
    Default initialization configuration.
    """
    return {
        "apps": {APP_NAME: {"default_connection": "default", "models": [__name__]}},
        "connections": {"default": connection},
        "routers": (),
        "timezone": "UTC",
        "use_tz": True,
    }


class URL(Model):
    """
    A URL.
    """

    class Meta(Model.Meta):
        """
        Model metadata.
        """

        abstract = True

    id = BigIntField(generated=True, index=True, pk=True, unique=True)
    """
    URL ID.
    """

    content = CharField(2047, index=True, unique=True)
    """
    The URL itself.

    The length limit 2047 is commonly used in search engines. See <https://stackoverflow.com/a/417184>.
    """

    redirect: ForeignKeyNullableRelation[Self] = ForeignKeyField(
        f"{APP_NAME}.URL",
        default=None,
        null=True,
        on_delete=RESTRICT,
        related_name=False,
    )
    """
    The URL to be redirected from this URL, if any.
    """

    page: OneToOneNullableRelation["Page"]
    """
    Corresponding page, if indexed.
    """

    inlinks: ManyToManyRelation["Page"]
    """
    Pages linking to this URL.
    """


class Page(Model):
    """
    An indexed page.
    """

    class Meta(Model.Meta):
        """
        Model metadata.
        """

        abstract = True

    id = BigIntField(generated=True, index=True, pk=True, unique=True)
    """
    Page ID.
    """

    url: OneToOneRelation[URL] = OneToOneField(
        f"{APP_NAME}.{URL.__name__}",
        related_name="page",
        on_delete=RESTRICT,
        index=True,
    )
    """
    URL of the page.
    """

    mod_time = DatetimeField()
    """
    Last modification time of the page, as reported by the server.
    """

    size = BigIntField(validators=(MinValueValidator(0),))
    """
    Size of the page, as reported by the server.
    """

    text = TextField()
    """
    Content of the page, including markups.
    """

    plaintext = TextField()
    """
    Plaintext content of the page, excluding markups.
    """

    title = TextField()
    """
    Title of the page.
    """

    outlinks: ManyToManyRelation["URL"] = ManyToManyField(
        f"{APP_NAME}.URL", on_delete=RESTRICT, related_name="inlinks"
    )
    """
    Links outgoing from this page.
    """

    @classmethod
    @atomic()
    async def index(cls, models: "Models", page: IndexedPage) -> bool:
        """
        Index an page and return whether the page is actually indexed.
        """
        urls = (str(page.url), *{str(link): ... for link in page.links})
        await models.URL.bulk_create(
            (models.URL(content=url) for url in urls),
            on_conflict=("content",),
            ignore_conflicts=True,
        )
        url_map = await models.URL.in_bulk(urls, "content")
        url = url_map.pop(urls[0])
        await url.fetch_related("page")
        if url.page is not None and url.page.mod_time >= page.mod_time:
            return False

        new_page = models.Page() if url.page is None else url.page
        new_page.update_from_dict(  # type: ignore
            {
                "url": url,
                "mod_time": page.mod_time,
                "text": page.text,
                "plaintext": page.plaintext,
                "size": page.size,
                "title": page.title,
            }
        )
        await new_page.save()
        await new_page.outlinks.clear()
        await new_page.outlinks.add(*url_map.values())

        # clear index
        await models.PageWord.filter(page=new_page).delete()

        # create words
        await models.Word.bulk_create(
            (
                models.Word(content=word)
                for word in chain(page.word_occurrences, page.word_occurrences_title)
            ),
            on_conflict=("content",),
            ignore_conflicts=True,
        )
        word_map = await models.Word.in_bulk(
            chain(page.word_occurrences, page.word_occurrences_title), "content"
        )

        # create page—word pairs
        pw_max_id = (
            await models.PageWord.annotate(_ret=Max("id", 0))
            .only("_ret")
            .get()
            .values_list("_ret", flat=True)
        ) or 0
        assert isinstance(pw_max_id, int)
        page_word_map = {
            word: models.PageWord(
                id=id,
                page=new_page,
                word=word,
            )
            for id, word in enumerate(word_map.values(), start=pw_max_id + 1)
        }
        await models.PageWord.bulk_create(page_word_map.values())

        # create positions
        await gather(
            models.WordPositions.bulk_create(
                models.WordPositions(
                    key_id=page_word_map[word_map[word_str]].id,
                    positions=",".join(map(str, wo.positions)),
                    frequency=wo.frequency,
                    tf_normalized=wo.tf_normalized,
                )
                for word_str, wo in page.word_occurrences.items()
            ),
            models.WordPositionsTitle.bulk_create(
                models.WordPositionsTitle(
                    key_id=page_word_map[word_map[word_str]].id,
                    positions=",".join(map(str, wo.positions)),
                    frequency=wo.frequency,
                    tf_normalized=wo.tf_normalized,
                )
                for word_str, wo in page.word_occurrences_title.items()
            ),
        )

        return True


class Word(Model):
    """
    An indexed word.
    """

    class Meta(Model.Meta):
        """
        Model metadata.
        """

        abstract = True

    id = BigIntField(generated=True, index=True, pk=True, unique=True)
    """
    Word ID.
    """

    content = CharField(255, index=True, unique=True)
    """
    The word itself.

    The length limit 255 is used to make it compatible with more database drivers.
    """

    # Precomputing the document frequency makes the indexing too complicated.
    #
    # df = BigIntField(default=0, validators=(MinValueValidator(0),))
    # """
    # Document frequency, the number of documents with this word. Considers plaintext only.
    # """

    # df_title = BigIntField(default=0, validators=(MinValueValidator(0),))
    # """
    # Document frequency, the number of documents with this word. Considers title only.
    # """


class PageWord(Model):
    """
    A page—word pair.
    """

    class Meta(Model.Meta):
        """
        Model metadata.
        """

        abstract = True
        indexes = (("page", "word"),)
        unique_together = (("page", "word"),)

    id = BigIntField(generated=True, index=True, pk=True, unique=True)
    """
    ID.
    """

    page: ForeignKeyRelation[Page] = ForeignKeyField(
        f"{APP_NAME}.{Page.__name__}", index=True, on_delete=RESTRICT
    )
    """
    The page the word is on.
    """

    word: ForeignKeyRelation[Word] = ForeignKeyField(
        f"{APP_NAME}.{Word.__name__}", index=True, on_delete=RESTRICT
    )
    """
    The word.
    """

    positions: OneToOneNullableRelation["WordPositions"]
    """
    Word positions for plaintext.
    """

    positions_title: OneToOneNullableRelation["WordPositionsTitle"]
    """
    Word positions for title.
    """


class WordPositionsType(StrEnum):
    """
    Type of word positions.
    """

    __slots__ = ()

    PLAINTEXT = "plaintext"
    """
    Represents word positions in plaintext.
    """
    TITLE = "title"
    """
    Represents word positions in title.
    """

    def model(self, models: "Models") -> type["WordPositions"]:
        match self:
            case self.PLAINTEXT:
                return models.WordPositions
            case self.TITLE:
                return models.WordPositionsTitle
            case _:  # type: ignore
                raise ValueError(self)


class WordPositions(Model):
    """
    Word positions for a page—word pair.
    """

    TYPE = WordPositionsType.PLAINTEXT
    """
    Type of word positions.
    """

    class Meta(Model.Meta):
        """
        Model metadata.
        """

        abstract = True
        indexes = (("key"),)
        unique_together = (("key",),)

    key: OneToOneRelation[PageWord] = OneToOneField(
        f"{APP_NAME}.{PageWord.__name__}",
        related_name="positions",
        on_delete=CASCADE,
        index=True,
    )
    """
    Corresponding page pair.
    """

    positions = TextField(validators=(CommaSeparatedIntegerListValidator(),))
    """
    Positions of the word occurrence on a page.

    Must not be empty, which is enforced by `CommaSeparatedIntegerListValidator`.
    """

    frequency = BigIntField(validators=(MinValueValidator(1),))
    """
    Frequency of the word in the page.
    """

    tf_normalized = FloatField(validators=(MinValueValidator(0), MaxValueValidator(1)))
    """
    Term frequency in the page, normalized.
    
    Calculated by (number of occurrences in the page / max number of occurrences of a word in the page).
    """


class WordPositionsTitle(WordPositions):
    """
    Word positions for a page—word pair. For titles.
    """

    TYPE = WordPositionsType.TITLE
    """
    Type of word positions.
    """

    class Meta(WordPositions.Meta):
        """
        Model metadata.
        """

        abstract = True

    key: OneToOneRelation[PageWord] = OneToOneField(
        f"{APP_NAME}.{PageWord.__name__}",
        related_name="positions_title",
        on_delete=CASCADE,
        index=True,
    )
    """
    Corresponding page pair.
    """


class Models(NamedTuple):
    Page: Type[Page]
    PageWord: Type[PageWord]
    URL: Type[URL]
    Word: Type[Word]
    WordPositions: Type[WordPositions]
    WordPositionsTitle: Type[WordPositionsTitle]


def new_model(model: type[_TExtendsModel]) -> type[_TExtendsModel]:
    """
    Create a new copy of a model.
    """
    return cast(type[_TExtendsModel], type(model.__name__, (model,), {}))


def new_models() -> Models:
    """
    Create new copies of the models.
    """
    return Models(
        new_model(Page),
        new_model(PageWord),
        new_model(URL),
        new_model(Word),
        new_model(WordPositions),
        new_model(WordPositionsTitle),
    )


MODELS = new_models()
"""
Default models.
"""

__models__ = MODELS
"""
Exported models.
"""
