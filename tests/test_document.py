import sys
import asyncio
from uuid import uuid4
from datetime import datetime

from preggy import expect

from jetengine import (
    Document,
    StringField,
    BooleanField,
    ListField,
    EmbeddedDocumentField,
    ReferenceField,
    DESCENDING,
    URLField,
    DateTimeField,
    UUIDField,
    IntField,
    JsonField,
)
from jetengine.errors import InvalidDocumentError, LoadReferencesRequiredError, UniqueKeyViolationError
from tests import AsyncTestCase, async_test


class User(Document):
    email = StringField(required=True)
    first_name = StringField(max_length=50, default=lambda: "Bernardo")
    last_name = StringField(max_length=50, default="Heynemann")
    is_admin = BooleanField(default=True)
    website = URLField(default="http://google.com/")
    updated_at = DateTimeField(required=True, auto_now_on_insert=True, auto_now_on_update=True)
    facebook_id = StringField(unique=True, sparse=True)

    def __repr__(self):
        return "%s %s <%s>" % (self.first_name, self.last_name, self.email)


class Employee(User):
    emp_number = StringField()


class Comment(Document):
    text = StringField(required=True)
    user = ReferenceField(User, required=True)


class CommentNotLazy(Document):
    __lazy__ = False

    text = StringField(required=True)
    user = ReferenceField(User, required=True)


class Post(Document):
    title = StringField(required=True)
    body = StringField(required=True)

    comments = ListField(EmbeddedDocumentField(Comment))


class TestDocument(AsyncTestCase):
    def setUp(self):
        super(TestDocument, self).setUp()
        self.drop_coll("User")
        self.drop_coll("Employee")
        self.drop_coll("Post")
        self.drop_coll("Comment")
        self.drop_coll("CommentNotLazy")

    def test_has_proper_collection(self):
        assert User.__collection__ == "User"

    def test_has_proper_objects(self):
        import jetengine.queryset

        expect(isinstance(User.objects, jetengine.queryset.QuerySet)).to_be_true()

    @async_test
    @asyncio.coroutine
    def test_can_create_new_instance(self):
        user = User(email="heynemann@gmail.com", first_name="Bernardo", last_name="Heynemann")
        result = yield from user.save()

        expect(result._id).not_to_be_null()
        expect(result.email).to_equal("heynemann@gmail.com")
        expect(result.first_name).to_equal("Bernardo")
        expect(result.last_name).to_equal("Heynemann")
        expect(result.facebook_id).to_be_null()

    @async_test
    @asyncio.coroutine
    def test_can_create_new_instance_with_defaults(self):
        user = User(email="heynemann@gmail.com")
        result = yield from user.save()

        expect(result._id).not_to_be_null()
        expect(result.email).to_equal("heynemann@gmail.com")
        expect(result.first_name).to_equal("Bernardo")
        expect(result.last_name).to_equal("Heynemann")
        expect(result.is_admin).to_be_true()

    @async_test
    @asyncio.coroutine
    def test_can_create_new_instance_with_defaults_and_db_fields(self):
        class Model(Document):
            last_name = StringField(db_field="db_last", default="Heynemann")
            first_name = StringField(db_field="db_first", default=lambda: "Bernardo")

        self.drop_coll_async(Model.__collection__)

        model = Model()
        result = yield from model.save()

        expect(result._id).not_to_be_null()
        expect(result.first_name).to_equal("Bernardo")
        expect(result.last_name).to_equal("Heynemann")

    @async_test
    @asyncio.coroutine
    def test_creating_invalid_instance_fails(self):
        user = User(email="heynemann@gmail.com", first_name="Bernardo", last_name="Heynemann", website="bla")
        try:
            yield from user.save()
        except InvalidDocumentError:
            err = sys.exc_info()[1]
            expect(err).to_have_an_error_message_of("Field 'website' must be valid.")
        else:
            assert False, "Should not have gotten this far"

        try:
            user = yield from User.objects.create(
                email="heynemann@gmail.com", first_name="Bernardo", last_name="Heynemann", website="bla"
            )
        except InvalidDocumentError:
            err = sys.exc_info()[1]
            expect(err).to_have_an_error_message_of("Field 'website' must be valid.")
        else:
            assert False, "Should not have gotten this far"

    @async_test
    @asyncio.coroutine
    def test_can_create_employee(self):
        user = Employee(
            email="heynemann@gmail.com", first_name="Bernardo", last_name="Heynemann", emp_number="Employee"
        )
        result = yield from user.save()

        expect(result._id).not_to_be_null()
        expect(result.email).to_equal("heynemann@gmail.com")
        expect(result.first_name).to_equal("Bernardo")
        expect(result.last_name).to_equal("Heynemann")
        expect(result.emp_number).to_equal("Employee")

    def test_duplicate_fields(self):
        try:

            class DuplicateField(User):
                email = StringField(required=True)

        except InvalidDocumentError:
            e = sys.exc_info()[1]
            expect(e).to_have_an_error_message_of("Multiple db_fields defined for: email ")
        else:
            assert False, "Should not have gotten this far."

    @async_test
    @asyncio.coroutine
    def test_can_update_employee(self):
        user = Employee(
            email="heynemann@gmail.com", first_name="Bernardo", last_name="Heynemann", emp_number="Employee"
        )
        user.emp_number = "12345"
        result = yield from user.save()

        expect(result._id).not_to_be_null()
        expect(result.email).to_equal("heynemann@gmail.com")
        expect(result.first_name).to_equal("Bernardo")
        expect(result.last_name).to_equal("Heynemann")
        expect(result.emp_number).to_equal("12345")

    @async_test
    @asyncio.coroutine
    def test_can_get_instance(self):
        user = Employee(
            email="heynemann@gmail.com", first_name="Bernardo", last_name="Heynemann", emp_number="Employee"
        )
        yield from user.save()

        retrieved_user = yield from Employee.objects.get(user._id)

        expect(retrieved_user._id).to_equal(user._id)
        expect(retrieved_user.email).to_equal("heynemann@gmail.com")
        expect(retrieved_user.first_name).to_equal("Bernardo")
        expect(retrieved_user.last_name).to_equal("Heynemann")
        expect(retrieved_user.emp_number).to_equal("Employee")
        expect(retrieved_user.is_admin).to_be_true()
        expect(retrieved_user.facebook_id).to_be_null()

    @async_test
    @asyncio.coroutine
    def test_can_get_instance_with_id_string(self):
        user = Employee(
            email="heynemann@gmail.com", first_name="Bernardo", last_name="Heynemann", emp_number="Employee"
        )
        yield from user.save()

        retrieved_user = yield from Employee.objects.get(str(user._id))

        expect(retrieved_user._id).to_equal(user._id)
        expect(retrieved_user.email).to_equal("heynemann@gmail.com")
        expect(retrieved_user.first_name).to_equal("Bernardo")
        expect(retrieved_user.last_name).to_equal("Heynemann")
        expect(retrieved_user.emp_number).to_equal("Employee")
        expect(retrieved_user.is_admin).to_be_true()

    @async_test
    @asyncio.coroutine
    def test_after_updated_get_proper_data(self):
        user = Employee(
            email="heynemann@gmail.com", first_name="Bernardo", last_name="Heynemann", emp_number="Employee"
        )
        yield from user.save()

        user.emp_number = "12345"
        yield from user.save()

        retrieved_user = yield from Employee.objects.get(user._id)

        expect(retrieved_user._id).to_equal(user._id)
        expect(retrieved_user.email).to_equal("heynemann@gmail.com")
        expect(retrieved_user.first_name).to_equal("Bernardo")
        expect(retrieved_user.last_name).to_equal("Heynemann")
        expect(retrieved_user.emp_number).to_equal("12345")

    @async_test
    @asyncio.coroutine
    def test_can_find_proper_document(self):
        yield from User.objects.create(email="heynemann@gmail.com", first_name="Bernardo", last_name="Heynemann")

        yield from User.objects.create(email="someone@gmail.com", first_name="Someone", last_name="Else")

        users = yield from User.objects.filter(email="someone@gmail.com").find_all()

        expect(users).to_be_instance_of(list)
        expect(users).to_length(1)

        first_user = users[0]
        expect(first_user.first_name).to_equal("Someone")
        expect(first_user.last_name).to_equal("Else")
        expect(first_user.email).to_equal("someone@gmail.com")

    @async_test
    @asyncio.coroutine
    def test_can_find_with_multiple_filters(self):
        yield from User.objects.create(email="heynemann@gmail.com", first_name="Bernardo", last_name="Heynemann")

        yield from User.objects.create(email="someone@gmail.com", first_name="Someone", last_name="Else")

        user = yield from User.objects.create(email="someone@gmail.com", first_name="Bernardo", last_name="Heynemann")

        last_user = yield from User.objects.create(email="other@gmail.com", first_name="Bernardo", last_name="Silva")

        # filter and filter not
        users = yield from User.objects.filter(email="someone@gmail.com").filter_not(first_name="Someone").find_all()

        expect(users).to_be_instance_of(list)
        expect(users).to_length(1)

        first_user = users[0]
        expect(first_user._id).to_equal(user._id)

        # filter and filter not for Q
        from jetengine import Q

        users = yield from User.objects.filter(email="someone@gmail.com").filter_not(Q(first_name="Someone")).find_all()

        expect(users).to_be_instance_of(list)
        expect(users).to_length(1)

        first_user = users[0]
        expect(first_user._id).to_equal(user._id)

        # filter not and filter not
        users = yield from User.objects.filter_not(last_name="Heynemann").filter_not(first_name="Someone").find_all()

        expect(users).to_be_instance_of(list)
        expect(users).to_length(1)

        first_user = users[0]
        expect(first_user._id).to_equal(last_user._id)

        # filter and filter
        users = yield from User.objects.filter(last_name="Silva").filter(first_name="Bernardo").find_all()

        expect(users).to_be_instance_of(list)
        expect(users).to_length(1)
        expect(users[0]._id).to_equal(last_user._id)

    @async_test
    @asyncio.coroutine
    def test_can_limit_number_of_documents(self):
        yield from User.objects.create(email="heynemann@gmail.com", first_name="Bernardo", last_name="Heynemann")

        yield from User.objects.create(email="someone@gmail.com", first_name="Someone", last_name="Else")

        users = yield from User.objects.limit(1).find_all()

        expect(users).to_be_instance_of(list)
        expect(users).to_length(1)

        first_user = users[0]
        expect(first_user.first_name).to_equal("Bernardo")
        expect(first_user.last_name).to_equal("Heynemann")
        expect(first_user.email).to_equal("heynemann@gmail.com")

    def test_cant_order_for_invalid_field(self):
        try:
            User.objects.order_by("invalid_field")
        except ValueError:
            err = sys.exc_info()[1]
            expect(err).to_have_an_error_message_of(
                "Invalid order by field 'invalid_field': Field not found in 'User'."
            )
        else:
            assert False, "Should not have gotten this far"

    @async_test
    @asyncio.coroutine
    def test_can_order_documents(self):
        yield from User.objects.create(email="heynemann@gmail.com", first_name="Bernardo", last_name="Heynemann")

        yield from User.objects.create(email="someone@gmail.com", first_name="Someone", last_name="Else")

        users = yield from User.objects.order_by("first_name", DESCENDING).find_all()

        expect(users).to_be_instance_of(list)
        expect(users).to_length(2)

        first_user = users[0]
        expect(first_user.first_name).to_equal("Someone")
        expect(first_user.last_name).to_equal("Else")
        expect(first_user.email).to_equal("someone@gmail.com")

    @async_test
    @asyncio.coroutine
    def test_can_order_documents_by_actual_field(self):
        yield from User.objects.create(email="heynemann@gmail.com", first_name="Bernardo", last_name="Heynemann")

        yield from User.objects.create(email="someone@gmail.com", first_name="Someone", last_name="Else")

        users_cursor = User.objects.order_by(User.first_name, DESCENDING)
        users = yield from users_cursor.find_all()

        expect(users).to_be_instance_of(list)
        expect(users).to_length(2)

        first_user = users[0]
        expect(first_user.first_name).to_equal("Someone")
        expect(first_user.last_name).to_equal("Else")
        expect(first_user.email).to_equal("someone@gmail.com")

    @async_test
    @asyncio.coroutine
    def test_can_count_documents(self):
        yield from User.objects.create(email="heynemann@gmail.com", first_name="Bernardo", last_name="Heynemann")

        yield from User.objects.create(email="someone@gmail.com", first_name="Someone", last_name="Else")

        user_count = yield from User.objects.count()
        expect(user_count).to_equal(2)

        user_count_cursor = User.objects.filter(email="someone@gmail.com")
        user_count = yield from user_count_cursor.count()
        expect(user_count).to_equal(1)

        user_count = yield from User.objects.filter(email="invalid@gmail.com").count()
        expect(user_count).to_equal(0)

    @async_test
    @asyncio.coroutine
    def test_saving_without_required_fields_raises(self):
        user = Employee(first_name="Bernardo", last_name="Heynemann", emp_number="Employee")
        try:
            yield from user.save()
        except InvalidDocumentError:
            err = sys.exc_info()[1]
            expect(err).to_have_an_error_message_of("Field 'email' is required.")

    @async_test
    @asyncio.coroutine
    def test_can_save_and_get_reference_with_lazy(self):
        user = yield from User.objects.create(email="heynemann@gmail.com", first_name="Bernardo", last_name="Heynemann")

        post = yield from Post.objects.create(title="Testing post", body="Testing post body")

        comment = Comment(text="Comment text for lazy test", user=user)
        post.comments.append(comment)
        yield from post.save()

        loaded_post = yield from Post.objects.get(post._id)

        result = yield from loaded_post.comments[0].load_references()

        expect(result["loaded_reference_count"]).to_equal(1)

    @async_test
    @asyncio.coroutine
    def test_can_save_and_get_specific_reference_with_lazy(self):
        user = yield from User.objects.create(email="heynemann@gmail.com", first_name="Bernardo", last_name="Heynemann")

        class ReferenceFieldClass(Document):
            ref1 = ReferenceField(User)
            ref2 = ReferenceField(User)
            ref3 = ReferenceField(User)

        ref = yield from ReferenceFieldClass.objects.create(ref1=user, ref2=user, ref3=user)

        loaded_ref = yield from ReferenceFieldClass.objects.get(ref._id)

        result = yield from loaded_ref.load_references(fields=["ref1"])

        expect(result["loaded_reference_count"]).to_equal(1)
        expect(loaded_ref.ref1._id).to_equal(user._id)

        try:
            assert loaded_ref.ref2._id
        except LoadReferencesRequiredError:
            err = sys.exc_info()[1]
            expect(err).to_have_an_error_message_of(
                "The property 'ref2' can't be accessed before calling 'load_references' on its instance first "
                "(ReferenceFieldClass) or setting __lazy__ to False in the ReferenceFieldClass class."
            )
        else:
            assert False, "Should not have gotten this far"

    @async_test
    @asyncio.coroutine
    def test_can_save_and_get_reference_with_find_all(self):
        user = yield from User.objects.create(email="heynemann@gmail.com", first_name="Bernardo", last_name="Heynemann")

        class ReferenceFieldClass(Document):
            __collection__ = "TestFindAllReferenceField"
            ref1 = ReferenceField(User)
            num = IntField(default=10)

        yield from ReferenceFieldClass.objects.delete()

        yield from ReferenceFieldClass.objects.create(ref1=user)

        yield from ReferenceFieldClass.objects.create(ref1=user)

        yield from ReferenceFieldClass.objects.create(ref1=user)

        result = yield from ReferenceFieldClass.objects.find_all(lazy=False)

        expect(result).to_length(3)
        expect(result[0].ref1._id).to_equal(user._id)

        ref_cursor = ReferenceFieldClass.objects.filter(num=20)
        result = yield from ref_cursor.find_all(lazy=False)

        expect(result).to_length(0)

    @async_test
    @asyncio.coroutine
    def test_can_save_and_get_reference_without_lazy(self):
        user = yield from User.objects.create(email="heynemann@gmail.com", first_name="Bernardo", last_name="Heynemann")

        comment = CommentNotLazy(text="Comment text", user=user)
        yield from comment.save()

        loaded_comment = yield from CommentNotLazy.objects.get(comment._id)

        expect(loaded_comment).not_to_be_null()
        expect(loaded_comment.user._id).to_equal(user._id)

        loaded_comments = yield from CommentNotLazy.objects.find_all()

        expect(loaded_comments).to_length(1)
        expect(loaded_comments[0].user._id).to_equal(user._id)

    @async_test
    @asyncio.coroutine
    def test_can_save_and_retrieve_blog_post(self):
        user = yield from User.objects.create(email="heynemann@gmail.com", first_name="Bernardo", last_name="Heynemann")

        post = yield from Post.objects.create(title="Testing post", body="Testing post body")

        post.comments.append(Comment(text="Comment text for blog post", user=user))
        yield from post.save()

        loaded_post = yield from Post.objects.get(post._id)

        expect(loaded_post).not_to_be_null()

        expect(loaded_post._id).to_equal(post._id)
        expect(loaded_post.title).to_equal("Testing post")
        expect(loaded_post.body).to_equal("Testing post body")

        expect(loaded_post.comments).to_length(1)
        expect(loaded_post.comments[0].text).to_equal("Comment text for blog post")

        try:
            loaded_post.comments[0].user
        except LoadReferencesRequiredError:
            err = sys.exc_info()[1]
            expected = (
                "The property 'user' can't be accessed before calling 'load_references' "
                + "on its instance first (Comment) or setting __lazy__ to False in the Comment class."
            )
            expect(err).to_have_an_error_message_of(expected)
        else:
            assert False, "Should not have gotten this far"

        result = yield from loaded_post.comments[0].load_references()

        loaded_reference_count = result["loaded_reference_count"]
        expect(loaded_reference_count).to_equal(1)

        expect(loaded_post.comments[0].user).to_be_instance_of(User)
        expect(loaded_post.comments[0].user._id).to_equal(user._id)
        expect(loaded_post.comments[0].user.email).to_equal("heynemann@gmail.com")
        expect(loaded_post.comments[0].user.first_name).to_equal("Bernardo")
        expect(loaded_post.comments[0].user.last_name).to_equal("Heynemann")

    @async_test
    @asyncio.coroutine
    def test_saving_a_loaded_post_updates_the_post(self):
        class LoadedPost(Document):
            uuid = UUIDField(default=uuid4)

        uuid = uuid4()

        post = yield from LoadedPost.objects.create(uuid=uuid)

        saved_post = yield from post.save()

        posts = yield from LoadedPost.objects.filter(uuid=uuid).find_all()

        expect(posts).to_length(1)
        expect(posts[0]._id).to_equal(post._id)
        expect(posts[0]._id).to_equal(saved_post._id)

    @async_test
    @asyncio.coroutine
    def test_saving_uses_default(self):
        class LoadedPost(Document):
            uuid = UUIDField(default=uuid4)

        post = yield from LoadedPost.objects.create()

        expect(post.uuid).not_to_be_null()

    @async_test
    @asyncio.coroutine
    def test_getting_by_field(self):
        class LoadedPost(Document):
            uuid = UUIDField(default=uuid4)

        uuid = uuid4()

        post = yield from LoadedPost.objects.create(uuid=uuid)

        loaded_post = yield from LoadedPost.objects.get(uuid=str(uuid))

        expect(loaded_post).not_to_be_null()
        expect(loaded_post._id).to_equal(post._id)

    def test_querying_by_invalid_operator(self):
        try:
            User.objects.filter(email__invalid="test")
        except ValueError:
            err = sys.exc_info()[1]
            expect(err).to_have_an_error_message_of(
                "Invalid filter 'email__invalid': Invalid operator (if this is a sub-property, "
                "then it must be used in embedded document fields)."
            )
        else:
            assert False, "Should not have gotten this far"

    @async_test
    @asyncio.coroutine
    def test_querying_by_lower_than(self):
        class Test(Document):
            __collection__ = "LowerThan"
            test = IntField()

        yield from Test.objects.delete()

        test = yield from Test.objects.create(test=10)

        yield from Test.objects.create(test=15)

        loaded_tests = yield from Test.objects.filter(test__lt=12).find_all()

        expect(loaded_tests).to_length(1)
        expect(loaded_tests[0]._id).to_equal(test._id)

        loaded_test = yield from Test.objects.get(test__lt=12)

        expect(loaded_test).not_to_be_null()
        expect(loaded_test._id).to_equal(test._id)

    @async_test
    @asyncio.coroutine
    def test_querying_by_greater_than(self):
        class Test(Document):
            __collection__ = "GreaterThan"
            test = IntField()

        yield from Test.objects.delete()

        yield from Test.objects.create(test=10)

        test = yield from Test.objects.create(test=15)

        loaded_tests = yield from Test.objects.filter(test__gt=12).find_all()

        expect(loaded_tests).to_length(1)
        expect(loaded_tests[0]._id).to_equal(test._id)

        loaded_test = yield from Test.objects.get(test__gt=12)

        expect(loaded_test).not_to_be_null()
        expect(loaded_test._id).to_equal(test._id)

    @async_test
    @asyncio.coroutine
    def test_querying_by_greater_than_or_equal(self):
        class Test(Document):
            __collection__ = "GreaterThanOrEqual"
            test = IntField()

        yield from Test.objects.delete()

        test = yield from Test.objects.create(test=10)

        test2 = yield from Test.objects.create(test=15)

        loaded_tests = yield from Test.objects.filter(test__gte=12).find_all()

        expect(loaded_tests).to_length(1)
        expect(loaded_tests[0]._id).to_equal(test2._id)

        loaded_tests = yield from Test.objects.filter(test__gte=10).find_all()

        expect(loaded_tests).to_length(2)
        expect(loaded_tests[0]._id).to_equal(test._id)
        expect(loaded_tests[1]._id).to_equal(test2._id)

    @async_test
    @asyncio.coroutine
    def test_querying_by_lesser_than_or_equal(self):
        class Test(Document):
            __collection__ = "LesserThanOrEqual"
            test = IntField()

        yield from Test.objects.delete()

        test = yield from Test.objects.create(test=10)

        test2 = yield from Test.objects.create(test=15)

        loaded_tests = yield from Test.objects.filter(test__lte=12).find_all()

        expect(loaded_tests).to_length(1)
        expect(loaded_tests[0]._id).to_equal(test._id)

        loaded_tests = yield from Test.objects.filter(test__lte=15).find_all()

        expect(loaded_tests).to_length(2)
        expect(loaded_tests[0]._id).to_equal(test._id)
        expect(loaded_tests[1]._id).to_equal(test2._id)

    @async_test
    @asyncio.coroutine
    def test_querying_by_exists(self):
        class Test2(Document):
            __collection__ = "EmbeddedExistsTest"
            test = IntField()

        class Test(Document):
            __collection__ = "EmbeddedExistsTestParent"
            test = ReferenceField(Test2)

        yield from Test.objects.delete()
        yield from Test2.objects.delete()

        test2 = yield from Test2.objects.create(test=10)

        test = yield from Test.objects.create(test=test2)

        yield from Test.objects.create()

        loaded_tests = yield from Test.objects.filter(test__exists=True).find_all()

        expect(loaded_tests).to_length(2)
        expect(loaded_tests[0]._id).to_equal(test._id)

    @async_test
    @asyncio.coroutine
    def test_querying_by_is_null(self):
        class Child(Document):
            __collection__ = "EmbeddedIsNullTest"
            num = IntField()

        class Parent(Document):
            __collection__ = "EmbeddedIsNullTestParent"
            child = ReferenceField(Child)

        yield from Parent.objects.delete()
        yield from Child.objects.delete()

        child = yield from Child.objects.create(num=10)

        parent = yield from Parent.objects.create(child=child)

        parent2 = yield from Parent.objects.create()

        parent_cursor = Parent.objects.filter(child__is_null=True)
        loaded_parents = yield from parent_cursor.find_all()

        expect(loaded_parents).to_length(1)
        expect(loaded_parents[0]._id).to_equal(parent2._id)

        parent_cursor = Parent.objects.filter(child__is_null=False)
        loaded_parents = yield from parent_cursor.find_all()

        expect(loaded_parents).to_length(1)
        expect(loaded_parents[0]._id).to_equal(parent._id)

    @async_test
    @asyncio.coroutine
    def test_querying_by_multiple_operators(self):
        class Child(Document):
            __collection__ = "MultipleOperatorsTest"
            num = IntField()

        yield from Child.objects.delete()

        child = yield from Child.objects.create(num=10)

        yield from Child.objects.create(num=7)

        yield from Child.objects.create(num=12)

        parent_cursor = Child.objects.filter(num__gt=8, num__lt=11)

        loaded_parents = yield from parent_cursor.find_all()

        expect(loaded_parents).to_length(1)
        expect(loaded_parents[0]._id).to_equal(child._id)

    @async_test
    @asyncio.coroutine
    def test_querying_by_not(self):
        class Child(Document):
            __collection__ = "NotOperatorTest"
            num = IntField()

        yield from Child.objects.delete()

        yield from Child.objects.create(num=10)

        child = yield from Child.objects.create(num=7)

        loaded_parents = yield from Child.objects.filter_not(num__gt=8).find_all()

        expect(loaded_parents).to_length(1)
        expect(loaded_parents[0]._id).to_equal(child._id)

        loaded_parents = yield from Child.objects.filter_not(num=10).find_all()

        expect(loaded_parents).to_length(1)
        expect(loaded_parents[0]._id).to_equal(child._id)

    @async_test
    @asyncio.coroutine
    def test_querying_by_in(self):
        dt1 = datetime(2010, 10, 10, 10, 10, 10)
        dt2 = datetime(2011, 10, 10, 10, 10, 10)
        dt3 = datetime(2012, 10, 10, 10, 10, 10)

        class Child(Document):
            __collection__ = "InOperatorTest"
            dt = DateTimeField()

        yield from Child.objects.delete()

        child = yield from Child.objects.create(dt=dt1)
        child2 = yield from Child.objects.create(dt=dt2)
        child3 = yield from Child.objects.create(dt=dt3)

        parent_cursor = Child.objects.filter(dt__in=[dt2, dt3])
        loaded_parents = yield from parent_cursor.find_all()

        expect(loaded_parents).to_length(2)
        expect(loaded_parents[0]._id).to_equal(child2._id)
        expect(loaded_parents[1]._id).to_equal(child3._id)

        parent_cursor = Child.objects.filter_not(dt__in=[dt2, dt3])
        loaded_parents = yield from parent_cursor.find_all()

        expect(loaded_parents).to_length(1)
        expect(loaded_parents[0]._id).to_equal(child._id)

    @async_test
    @asyncio.coroutine
    def test_querying_in_an_embedded_document(self):
        class TestEmbedded(Document):
            __collection__ = "TestEmbedded"
            num = IntField()

        class Test(Document):
            __collection__ = "TestEmbeddedParent"
            test = EmbeddedDocumentField(TestEmbedded)

        yield from Test.objects.delete()

        test = yield from Test.objects.create(test=TestEmbedded(num=10))

        yield from Test.objects.create(test=TestEmbedded(num=15))

        loaded_tests = yield from Test.objects.filter(test__num__lte=12).find_all()

        expect(loaded_tests).to_length(1)
        expect(loaded_tests[0]._id).to_equal(test._id)

        loaded_tests = yield from Test.objects.filter(test__num=10).find_all()

        expect(loaded_tests).to_length(1)
        expect(loaded_tests[0]._id).to_equal(test._id)

    @async_test
    @asyncio.coroutine
    def test_can_update_multiple_documents(self):
        yield from User.objects.create(email="email@gmail.com", first_name="First", last_name="Last2")
        yield from User.objects.create(email="email2@gmail.com", first_name="First2", last_name="Last2")
        yield from User.objects.create(email="email3@gmail.com", first_name="First3", last_name="Last3")
        yield from User.objects.create(email="email4@gmail.com", first_name="First4", last_name="Last4")

        result = yield from User.objects.filter(first_name__in=["First2", "First3"]).update({User.first_name: "Second"})

        expect(result.count).to_equal(2)
        expect(result.updated_existing).to_be_true()

        count = yield from User.objects.filter(first_name="Second").count()

        expect(count).to_equal(2)

        result = yield from User.objects.update({User.last_name: "NewLast"})

        expect(result.count).to_equal(4)
        expect(result.updated_existing).to_be_true()

        count = yield from User.objects.filter(last_name="NewLast").count()

        expect(count).to_equal(4)

    @async_test
    @asyncio.coroutine
    def test_skip(self):
        yield from User.objects.create(email="email@gmail.com", first_name="First", last_name="Last")
        yield from User.objects.create(email="email2@gmail.com", first_name="First2", last_name="Last2")
        yield from User.objects.create(email="email3@gmail.com", first_name="First3", last_name="Last3")
        yield from User.objects.create(email="email4@gmail.com", first_name="First4", last_name="Last4")

        users_cursor = User.objects.order_by(User.email).skip(2).limit(1)
        users = yield from users_cursor.find_all()

        expect(users).to_length(1)
        # TODO check if users[0] == user3

    @async_test
    @asyncio.coroutine
    def test_on_save_field(self):
        class SizeDocument(Document):
            items = ListField(IntField())
            item_size = IntField(default=0, on_save=lambda doc, creating: len(doc.items))

        doc = yield from SizeDocument.objects.create(items=[1, 2, 3])

        loaded = yield from SizeDocument.objects.get(doc._id)

        expect(loaded.item_size).to_equal(3)

        loaded.items = [1, 2, 3, 4, 5]
        yield from loaded.save()

        loaded = yield from SizeDocument.objects.get(doc._id)

        expect(loaded.item_size).to_equal(5)

    @async_test
    @asyncio.coroutine
    def test_unique_field(self):
        class UniqueFieldDocument(Document):
            name = StringField(unique=True)

        # yield from UniqueFieldDocument.objects.delete()
        yield from self.drop_coll_async(UniqueFieldDocument.__collection__)

        yield from UniqueFieldDocument.ensure_index()

        yield from UniqueFieldDocument.objects.create(name="test")

        # msg = 'The index "test.UniqueFieldDocument.$name_1" was violated when trying to save this "UniqueFieldDocument" (error code: E11000).'
        with expect.error_to_happen(UniqueKeyViolationError):
            yield from UniqueFieldDocument.objects.create(name="test")

        doc = yield from UniqueFieldDocument.objects.create(name="test2")

        doc.name = "test"
        with expect.error_to_happen(UniqueKeyViolationError):
            yield from doc.save()

    @async_test
    @asyncio.coroutine
    def test_unique_sparse_field(self):
        class UniqueSparseFieldDocument(Document):
            unique_id = StringField(unique=True, sparse=True)

        # yield from UniqueSparseFieldDocument.objects.delete()
        yield from self.drop_coll_async(UniqueSparseFieldDocument.__collection__)

        yield from UniqueSparseFieldDocument.ensure_index()

        yield from UniqueSparseFieldDocument.objects.create(unique_id=None)

        try:
            yield from UniqueSparseFieldDocument.objects.create(unique_id=None)
        except UniqueKeyViolationError:
            assert False, "UniqueKeyViolationError should not be raised for unique sparse field with empty value"

    @async_test
    @asyncio.coroutine
    def test_json_field_with_document(self):
        class JSONFieldDocument(Document):
            field = JsonField()

        obj = [{"a": 1}, {"b": 2}]

        doc = yield from JSONFieldDocument.objects.create(field=obj)

        loaded = yield from JSONFieldDocument.objects.get(doc._id)

        expect(loaded.field).to_be_like([{"a": 1}, {"b": 2}])

    @async_test
    @asyncio.coroutine
    def test_dynamic_fields(self):
        class DynamicFieldDocument(Document):
            __collection__ = "TestDynamicFieldDocument"

        yield from self.drop_coll_async(DynamicFieldDocument.__collection__)

        obj = {"a": 1, "b": 2}

        doc = yield from DynamicFieldDocument.objects.create(**obj)

        expect(doc._id).not_to_be_null()
        expect(doc.a).to_equal(1)
        expect(doc.b).to_equal(2)

        loaded_document = yield from DynamicFieldDocument.objects.get(doc._id)

        expect(loaded_document.a).to_equal(1)
        expect(loaded_document.b).to_equal(2)

    @async_test
    @asyncio.coroutine
    def test_dynamic_fields_when_saving(self):
        class DynamicFieldDocument(Document):
            __collection__ = "TestDynamicFieldDocumentWhenSaving"

        yield from self.drop_coll_async(DynamicFieldDocument.__collection__)

        doc = DynamicFieldDocument()
        doc.a = 1
        doc.b = 2
        doc = yield from doc.save()

        expect(doc._id).not_to_be_null()
        expect(doc.a).to_equal(1)
        expect(doc.b).to_equal(2)

        loaded_document = yield from DynamicFieldDocument.objects.get(doc._id)

        expect(loaded_document.a).to_equal(1)
        expect(loaded_document.b).to_equal(2)

    @async_test
    @asyncio.coroutine
    def test_dynamic_fields_multiple_value(self):
        class DynamicFieldDocument(Document):
            __collection__ = "TestDynamicFieldDocumentMultipleValue"

        yield from self.drop_coll_async(DynamicFieldDocument.__collection__)

        doc = DynamicFieldDocument()
        doc.a = [1, 2, 3, 4]
        doc = yield from doc.save()

        expect(doc._id).not_to_be_null()
        expect(doc.a).to_be_like([1, 2, 3, 4])

        loaded_document = yield from DynamicFieldDocument.objects.get(a=[1, 2, 3, 4])

        expect(loaded_document._id).to_equal(doc._id)

        loaded_document = yield from DynamicFieldDocument.objects.get(a=1)

        expect(loaded_document._id).to_equal(doc._id)

    @async_test
    @asyncio.coroutine
    def test_dynamic_fields_query(self):
        class DynamicFieldDocument(Document):
            __collection__ = "TestDynamicFieldDocumentQuery"

        yield from self.drop_coll_async(DynamicFieldDocument.__collection__)

        obj = {"a": 1, "b": 2}

        doc = yield from DynamicFieldDocument.objects.create(**obj)

        expect(doc._id).not_to_be_null()
        expect(doc.a).to_equal(1)

        document_count_cursor = DynamicFieldDocument.objects.filter(**obj)
        document_count = yield from document_count_cursor.count()

        expect(document_count).to_equal(1)

    @async_test
    @asyncio.coroutine
    def test_dynamic_fields_with_two_version_fields(self):
        class Version1Document(Document):
            __collection__ = "TestDynamicFieldDocumentQuery1"
            old_element = StringField(default="old_string_field")

        class Version2Document(Document):
            __collection__ = "TestDynamicFieldDocumentQuery1"
            old_element = StringField(default="old_string_field")
            new_element = StringField(default="new_string_field")

        yield from self.drop_coll_async(Version1Document.__collection__)

        doc1 = Version1Document()
        doc1.old_element = "my_old_string_field1"
        doc1 = yield from doc1.save()

        doc2 = Version2Document()
        doc2.old_element = "my_old_string_field2"
        doc2.new_element = "my_new_string_field2"
        doc2 = yield from doc2.save()

        # Querying with the old Version1.
        # This effect happens when you have 2 different services using different versions of the document.
        # When editing the version1 document, no _dynfield value should be added because version2 will
        # eventually overwrite real values from new_field.
        doc2_with_version1 = yield from Version1Document.objects.get(old_element="my_old_string_field2")

        expect(doc2_with_version1._id).not_to_be_null()
        expect(doc2_with_version1.old_element).to_equal("my_old_string_field2")
        expect(doc2_with_version1.new_element).to_equal("my_new_string_field2")

        doc2_with_version1 = yield from Version1Document.objects.get(old_element="my_old_string_field2")

        expect(doc2_with_version1._id).not_to_be_null()
        expect(doc2_with_version1.old_element).to_equal("my_old_string_field2")
        expect(doc2_with_version1.new_element).to_equal("my_new_string_field2")

        # Changing one field and saving it.
        doc2_with_version1.old_element = "my_old_string_field2_modified"
        doc2_with_version1 = yield from doc2_with_version1.save()
        # The database should contain the dynamic field.
        # Querying with the new version should not overwrite the data.
        doc2_with_version2 = yield from Version2Document.objects.get(old_element="my_old_string_field2_modified")
        expect(doc2_with_version2._id).not_to_be_null()
        expect(doc2_with_version2.old_element).to_equal("my_old_string_field2_modified")

        doc2_with_version2 = yield from doc2_with_version2.save()

        # After saving the new version of the document it should stay the way it was designed to be
        expect(doc2_with_version2.new_element).to_equal("my_new_string_field2")

    @async_test
    @asyncio.coroutine
    def test_can_query_by_elem_match(self):
        class ElemMatchDocument(Document):
            items = ListField(IntField())

        yield from self.drop_coll_async(ElemMatchDocument.__collection__)

        doc = yield from ElemMatchDocument.objects.create(items=[1, 2, 3, 4])

        loaded_document = yield from ElemMatchDocument.objects.get(items=1)

        expect(loaded_document._id).to_equal(doc._id)

    @async_test
    @asyncio.coroutine
    def test_can_query_by_elem_match_when_list_of_embedded(self):
        class ElemMatchEmbeddedDocument(Document):
            name = StringField()

        class ElemMatchEmbeddedParentDocument(Document):
            items = ListField(EmbeddedDocumentField(ElemMatchEmbeddedDocument))

        yield from self.drop_coll_async(ElemMatchEmbeddedDocument.__collection__)
        yield from self.drop_coll_async(ElemMatchEmbeddedParentDocument.__collection__)

        yield from ElemMatchEmbeddedParentDocument.objects.create(
            items=[ElemMatchEmbeddedDocument(name="a"), ElemMatchEmbeddedDocument(name="b")]
        )

        yield from ElemMatchEmbeddedParentDocument.objects.create(
            items=[ElemMatchEmbeddedDocument(name="c"), ElemMatchEmbeddedDocument(name="d")]
        )

        docs_cursor = ElemMatchEmbeddedParentDocument.objects
        docs_cursor.filter(items__name="b")
        loaded_document = yield from docs_cursor.find_all()

        expect(loaded_document).to_length(1)

    @async_test
    @asyncio.coroutine
    def test_raw_query(self):
        class RawQueryEmbeddedDocument(Document):
            name = StringField()

        class RawQueryDocument(Document):
            items = ListField(EmbeddedDocumentField(RawQueryEmbeddedDocument))

        yield from self.drop_coll_async(RawQueryEmbeddedDocument.__collection__)
        yield from self.drop_coll_async(RawQueryDocument.__collection__)

        yield from RawQueryDocument.objects.create(
            items=[RawQueryEmbeddedDocument(name="a"), RawQueryEmbeddedDocument(name="b")]
        )

        yield from RawQueryDocument.objects.create(
            items=[RawQueryEmbeddedDocument(name="c"), RawQueryEmbeddedDocument(name="d")]
        )

        items_cursor = RawQueryDocument.objects.filter({"items.name": "a"})
        items = yield from items_cursor.find_all()

        expect(items).to_length(1)

    @async_test
    @asyncio.coroutine
    def test_list_field_with_reference_field(self):
        class Ref(Document):
            __collection__ = "ref"
            val = StringField()

        class Base(Document):
            __collection__ = "base"
            list_val = ListField(ReferenceField(reference_document_type=Ref))

        yield from Ref.objects.delete()
        yield from Base.objects.delete()

        ref1 = yield from Ref.objects.create(val="v1")
        ref2 = yield from Ref.objects.create(val="v2")
        ref3 = yield from Ref.objects.create(val="v3")

        base = yield from Base.objects.create(list_val=[ref1, ref2, ref3])

        base = yield from Base.objects.get(base._id)
        expect(base).not_to_be_null()

        yield from base.load_references()
        expect(base.list_val).to_length(3)
        expect(base.list_val[0]).to_be_instance_of(Ref)

    @async_test
    @asyncio.coroutine
    def test_list_field_with_reference_field_without_lazy(self):
        class Ref(Document):
            __collection__ = "ref"
            val = StringField()

        class Base(Document):
            __collection__ = "base"
            __lazy__ = False
            list_val = ListField(ReferenceField(reference_document_type=Ref))

        yield from Ref.objects.delete()
        yield from Base.objects.delete()

        ref1 = yield from Ref.objects.create(val="v1")
        ref2 = yield from Ref.objects.create(val="v2")
        ref3 = yield from Ref.objects.create(val="v3")

        base = yield from Base.objects.create(list_val=[ref1, ref2, ref3])

        base = yield from Base.objects.get(base._id)
        expect(base).not_to_be_null()

        expect(base.list_val).to_length(3)
        expect(base.list_val[0]).to_be_instance_of(Ref)
