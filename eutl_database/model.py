from datetime import datetime

from sqlalchemy import (
    Integer,
    Float,
    Column,
    String,
    ForeignKey,
    Boolean,
    DateTime,
    BigInteger,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

Base = declarative_base()


class Transaction(Base):
    """Transaction blocks"""

    __tablename__ = "transaction"

    id = Column(Integer, primary_key=True, autoincrement=True)
    transactionID = Column(String(100))
    transactionBlock = Column(Integer())
    tradingSystem = Column(String(20))
    date = Column(DateTime)
    acquiringYear = Column(Integer())
    transferringYear = Column(Integer())
    transactionTypeMain_id = Column(
        Integer, ForeignKey("transaction_type_main_code.id")
    )
    transactionTypeSupplementary_id = Column(
        Integer, ForeignKey("transaction_type_supplementary_code.id")
    )
    transferringAccount_id = Column(Integer, ForeignKey("account.id"))
    acquiringAccount_id = Column(Integer, ForeignKey("account.id"))
    unitType_id = Column(String(25), ForeignKey("unit_type_code.id"))
    project_id = Column(Integer(), ForeignKey("offset_project.id"))
    amount = Column(BigInteger())
    created_on = Column(DateTime(), default=datetime.now)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)

    # relations
    transferringAccount = relationship(
        "Account",
        foreign_keys=[transferringAccount_id],
        backref="transferringTransactions",
        lazy=True,
    )
    acquiringAccount = relationship(
        "Account",
        foreign_keys=[acquiringAccount_id],
        backref="acquiringTransactions",
        lazy=True,
    )
    unitType = relationship("UnitType", lazy=True)
    project = relationship("OffsetProject", backref="transactions", lazy=True)
    transactionTypeMain = relationship(
        "TransactionTypeMain", lazy=True, backref="transactions"
    )
    transactionTypeSupplementary = relationship(
        "TransactionTypeSupplementary", lazy=True, backref="transactions"
    )

    def __repr__(self):
        return "<Transaction(%r, %r, %r, %r, %r)>" % (
            self.id,
            self.date,
            self.transferringAccount_id,
            self.acquiringAccount_id,
            self.amount,
        )


class Account(Base):
    __tablename__ = "account"

    id = Column(Integer(), primary_key=True)
    tradingSystem = Column(String(20))
    accountIDEutl = Column(Integer())
    accountIDTransactions = Column(String(50))
    accountIDESD = Column(String(50))
    yearValid = Column(Integer())
    name = Column(String(250))
    registry_id = Column(String(10), ForeignKey("country_code.id"), index=True)
    accountHolder_id = Column(Integer(), ForeignKey("account_holder.id"), index=True)
    accountType_id = Column(String(10), ForeignKey("account_type_code.id"), index=True)
    isOpen = Column(Boolean())
    openingDate = Column(DateTime())
    closingDate = Column(DateTime())
    commitmentPeriod = Column(String(100))
    companyRegistrationNumber = Column(String(250))
    companyRegistrationNumberType = Column(String(250))
    isRegisteredEutl = Column(Boolean(), default=True)
    installation_id = Column(String(100), ForeignKey("installation.id"), index=True)
    bvdId = Column(String(100))
    created_on = Column(DateTime(), default=datetime.now)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)

    # relations
    accountType = relationship("AccountType", backref="accounts")
    registry = relationship("CountryCode", backref="accounts")
    installation = relationship("Installation", back_populates="accounts")
    accountHolder = relationship("AccountHolder", back_populates="accounts")

    # transferringTransactions --> all transactions with account as transferring side
    # acquiringTransactions --> all transactions with account as acquiring side

    def __repr__(self):
        return "<Account(%r, %r, %r, %r)>" % (
            self.id,
            self.name,
            self.registry_id,
            self.accountType_id,
        )


class AccountHolder(Base):
    __tablename__ = "account_holder"
    id = Column(Integer(), primary_key=True)
    tradingSystem = Column(String(20))
    name = Column(String(300))
    addressMain = Column(String(300))
    addressSecondary = Column(String(300))
    postalCode = Column(String(300))
    city = Column(String(300))
    telephone1 = Column(String(300))
    telephone2 = Column(String(300))
    eMail = Column(String(300))
    legalEntityIdentifier = Column(String(300))
    country_id = Column(String(300), ForeignKey("country_code.id"), index=True)
    created_on = Column(DateTime(), default=datetime.now)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)

    country = relationship("CountryCode", backref="accountHolders", lazy=True)
    accounts = relationship("Account", back_populates="accountHolder", lazy=True)

    def __repr__(self):
        return "<AccountHolder(%r, %r, %r)>" % (self.id, self.name, self.country_id)


class Installation(Base):
    """EUTL regulated entity"""

    __tablename__ = "installation"

    id = Column(String(20), primary_key=True)
    name = Column(String(250))
    registry_id = Column(String(2), ForeignKey("country_code.id"), index=True)
    activity_id = Column(
        Integer(), ForeignKey("activity_type_code.id"), nullable=False, index=True
    )
    eprtrID = Column(String(200))
    parentCompany = Column(String(250))
    subsidiaryCompany = Column(String(1000))
    permitID = Column(String(250))
    designatorICAO = Column(String(250))
    monitoringID = Column(String(250))
    monitoringExpiry = Column(String())
    monitoringFirstYear = Column(String(250))
    permitDateExpiry = Column(DateTime())
    isAircraftOperator = Column(Boolean())
    ec748_2009Code = Column(String(100))
    permitDateEntry = Column(DateTime())
    mainAddress = Column(String(250))
    secondaryAddress = Column(String(250))
    postalCode = Column(String(250))
    city = Column(String(250))
    country_id = Column(String(25), ForeignKey("country_code.id"), index=True)
    latitudeEutl = Column(Float())
    longitudeEutl = Column(Float())
    latitudeGoogle = Column(Float())
    longitudeGoogle = Column(Float())
    nace15_id = Column(String(10))
    nace20_id = Column(String(10))
    nace_id = Column(String(10), ForeignKey("nace_code.id"), index=True)
    euEntitlement = Column(Integer())
    chEntitlement = Column(Integer())
    created_on = Column(DateTime(), default=datetime.now)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)

    # relationships
    activityType = relationship(
        "ActivityType",
        backref="installations_in_country",
        foreign_keys=[activity_id],
        lazy=True,
    )
    registry = relationship(
        "CountryCode",
        backref="installations_in_registry",
        foreign_keys=[registry_id],
        lazy=True,
    )
    country = relationship(
        "CountryCode",
        backref="installations_in_country",
        foreign_keys=[country_id],
        lazy=True,
    )
    # compliance = relationship("Compliance", backref="installation")
    # surrendering = relationship("Surrender", backref="installation")
    accounts = relationship("Account", back_populates="installation")
    # nace = relationship("NaceCode", backref="installations")

    def __repr__(self):
        return "<Installation(%r, %r, %r)>" % (self.id, self.name, self.registry)


class Compliance(Base):
    """compliance data"""

    __tablename__ = "compliance"

    installation_id = Column(
        String(100), ForeignKey("installation.id"), primary_key=True
    )
    year = Column(Integer(), primary_key=True)
    reportedInSystem = Column(String(10), primary_key=True)
    euetsPhase = Column(String(100))
    compliance_id = Column(String(100), ForeignKey("compliance_code.id"))
    allocatedFree = Column(Integer())
    allocatedNewEntrance = Column(Integer())
    allocatedTotal = Column(Integer())
    allocated10c = Column(Integer())
    verified = Column(Integer())
    verifiedCummulative = Column(Integer())
    verifiedUpdated = Column(Boolean())
    surrendered = Column(Integer())
    surrenderedCummulative = Column(Integer())
    created_on = Column(DateTime(), default=datetime.now)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)

    # relations
    installation = relationship("Installation", backref="compliance", lazy=True)
    compliance = relationship("ComplianceCode", lazy=True)

    def __repr__(self):
        return "<Compliance(%r, %r): allocated: %r, surrendered: %r, verified: %r>" % (
            self.installation_id,
            self.year,
            self.allocatedTotal,
            self.surrendered,
            self.verified,
        )


class EsdCompliance(Base):
    """compliance data for effor sharing"""

    __tablename__ = "esd_compliance"
    account_id = Column(Integer(), ForeignKey("account.id"), primary_key=True)
    year = Column(Integer(), primary_key=True)
    memberstate_id = Column(String(10), ForeignKey("country_code.id"))
    balance = Column(Integer())
    penalty = Column(Integer())
    allocated = Column(Integer())
    verified = Column(Integer())
    surrendered = Column(Integer())
    surrenderedAea = Column(Integer())
    surrenderedCredits = Column(Integer())
    compliance_id = Column(String(100), ForeignKey("compliance_code.id"))
    created_on = Column(DateTime(), default=datetime.now)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)


class Surrender(Base):
    """surrendering details"""

    __tablename__ = "surrender"
    id = Column(Integer, primary_key=True)
    installation_id = Column(String(100), ForeignKey("installation.id"))
    reportedInSystem = Column(String(10), primary_key=True)
    year = Column(Integer())
    unitType_id = Column(String(25), ForeignKey("unit_type_code.id"))
    amount = Column(Integer())
    originatingRegistry_id = Column(String(10), ForeignKey("country_code.id"))
    project_id = Column(Integer(), ForeignKey("offset_project.id"))
    created_on = Column(DateTime(), default=datetime.now)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)

    # relations
    installation = relationship("Installation", backref="surrendering", lazy=True)
    unitType = relationship("UnitType", lazy=True)
    originatingCountry = relationship("CountryCode", lazy=True)
    project = relationship("OffsetProject", backref="surrendering", lazy=True)

    def __repr__(self):
        return "<Surrendering(%r, %r)>" % (self.installation_id, self.year)


class OffsetProject(Base):
    """ERU and CER projects"""

    __tablename__ = "offset_project"
    id = Column(Integer(), primary_key=True)
    track = Column(Integer())
    country_id = Column(String(10), ForeignKey("country_code.id"))
    source = Column(String(50))
    created_on = Column(DateTime(), default=datetime.now)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)

    # relations
    country = relationship("CountryCode", backref="offsetProjects", lazy=True)
    # transactions = relationship("Transaction", lazy=True)

    def __repr__(self):
        return "<OffsetProject(%r, %r, %r)>" % (self.id, self.track, self.country_id)


class TransactionTypeMain(Base):
    """Lookup table for main transaction type"""

    __tablename__ = "transaction_type_main_code"

    id = Column(Integer, primary_key=True)
    description = Column(String(250), nullable=False)

    def __repr__(self):
        return "<TransactionTypeMain(%r, %r)>" % (self.id, self.description)


class TransactionTypeSupplementary(Base):
    """Supplementary transaction type"""

    __tablename__ = "transaction_type_supplementary_code"

    id = Column(Integer, primary_key=True)
    description = Column(String(250), nullable=False)

    def __repr__(self):
        return "<TransactionTypeSupplementary(%r, %r)>" % (self.id, self.description)


class AccountType(Base):
    """look-up table for account types"""

    __tablename__ = "account_type_code"

    id = Column(String(10), primary_key=True)
    description = Column(String(250), nullable=False)

    def __repr__(self):
        return "<AccountType(%r, %r)>" % (self.id, self.description)


class ActivityType(Base):
    """Lookup table for account types"""

    __tablename__ = "activity_type_code"

    id = Column(Integer(), primary_key=True)
    description = Column(String(250), nullable=False)

    def __repr__(self):
        return "<ActivityType(%r, %r)>" % (self.id, self.description)


class UnitType(Base):
    """Lookup table for allowances unit types"""

    __tablename__ = "unit_type_code"

    id = Column(String(25), primary_key=True)
    description = Column(String(250), nullable=False)

    def __repr__(self):
        return "<UnitType(%r, %r)>" % (self.id, self.description)


class CountryCode(Base):
    """Lookup table for countries"""

    __tablename__ = "country_code"

    id = Column(String(10), primary_key=True)
    description = Column(String(250), nullable=False)

    def __repr__(self):
        return "<CountryCode(%r, %r)>" % (self.id, self.description)


class ComplianceCode(Base):
    """Lookup table for compliance status"""

    __tablename__ = "compliance_code"

    id = Column(String(10), primary_key=True)
    description = Column(String(250), nullable=False)

    def __repr__(self):
        return "<ComplianceCode(%r, %r)>" % (self.id, self.description)


class NaceCode(Base):
    __tablename__ = "nace_code"
    id = Column(String(10), primary_key=True)
    parent_id = Column(String(10), ForeignKey("nace_code.id"))
    level = Column(Integer)
    description = Column(String(50000))
    includes = Column(String(50000))
    includesAlso = Column(String(50000))
    ruling = Column(String(50000))
    excludes = Column(String(50000))
    isic4_id = Column(String(10))

    childs = relationship("NaceCode", backref=backref("parent", remote_side=[id]))
    # installations = relationship("Installation", foreign_keys=[nace])

    def __repr__(self):
        return "<NaceCode(%r, %r)>" % (self.id, self.description)
