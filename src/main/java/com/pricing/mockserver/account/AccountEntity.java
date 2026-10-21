package com.pricing.mockserver.account;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import com.pricing.mockserver.accountattribute.AccountAttributeEntity;
import com.pricing.mockserver.batch.BatchOutboxEntity;
import com.pricing.mockserver.feerequest.FeeRequestEntity;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@AllArgsConstructor
@Builder
@Getter
@Setter
@NoArgsConstructor
@Table(name = "accounts")
public class AccountEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@ManyToOne(fetch = FetchType.LAZY, optional = false)
	@JoinColumn(name = "batch_id", nullable = false)
	private BatchOutboxEntity batch;

	@Column(name = "account_number", nullable = false, length = 25)
	private String accountNumber;

	@Column(name = "product_code", nullable = false, length = 25)
	private String productCode;

	@Column(name = "branch_code", nullable = false, length = 25)
	private String branchCode;

	@Column(name = "pricing_date", nullable = false)
	private LocalDate pricingDate;

	@Default
	@OneToMany(mappedBy = "account", cascade = CascadeType.ALL, orphanRemoval = true)
	private List<AccountAttributeEntity> attributes = new ArrayList<>();

	@Default
	@OneToMany(mappedBy = "account", cascade = CascadeType.ALL, orphanRemoval = true)
	private List<FeeRequestEntity> fees = new ArrayList<>();
}
